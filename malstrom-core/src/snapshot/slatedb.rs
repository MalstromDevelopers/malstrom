use std::cell::RefCell;
use std::time::Duration;
use std::{pin::pin, sync::Arc};

use super::{PersistenceBackend, PersistenceClient, SnapshotVersion};
use crate::types::WorkerId;
use bytes::Buf;
use object_store::PutPayload;
use object_store::{path::Path, ObjectStore};
use slatedb::config::DbOptions;
use slatedb::db::Db;
use slatedb::error::SlateDBError;
use thiserror::Error;
use tokio::runtime::{Handle, Runtime};
use tokio_stream::StreamExt;
use tracing::debug;

/// A snapshot persistence backend utilizing [SlateDB](slatedb.io)
/// for saving snapshots to object stores or local disk
pub struct SlateDbBackend {
    base_path: Path,
    object_store: Arc<dyn ObjectStore>,
    commits: Commits,
    rt: Runtime,
}

impl PersistenceBackend for SlateDbBackend {
    type Client = SlateDbClient;

    fn last_commited(&self, worker_id: WorkerId) -> Self::Client {
        let last_commit = self.commits.get_last_commited().unwrap_or(0);
        self.for_version(worker_id, &last_commit)
    }

    fn for_version(&self, worker_id: WorkerId, snapshot_version: &SnapshotVersion) -> Self::Client {
        let version_is_committed = self.commits.is_commited(snapshot_version);

        let db_path = self
            .base_path
            .child("snapshots")
            .child(format!("worker{}", worker_id))
            .child(format!("version{}", snapshot_version));
        if !version_is_committed {
            // either it is a completely new version or we wrote it and did not
            // commit, in either case we want a clean directory
            match self.rt.block_on(delete_dir(&self.object_store, &db_path)) {
                Ok(_) => (),
                Err(object_store::Error::NotFound { path: _, source: _ }) => (),
                e => e.unwrap(),
            }
        }
        let db_open = Db::open(db_path, Arc::clone(&self.object_store));
        let snapshot_db = self
            .rt
            .block_on(db_open)
            .expect("Expected to open snapshot db");
        SlateDbClient::new(
            snapshot_db,
            *snapshot_version,
            self.rt.handle().clone().to_owned(),
        )
    }

    fn commit_version(&self, snapshot_version: &SnapshotVersion) {
        self.commits
            .commit(*snapshot_version)
            .expect("Commit new version");
    }
}

impl SlateDbBackend {
    // create a new backend at the given path and filesystem
    pub fn new(
        object_store: Arc<dyn ObjectStore>,
        base_path: Path,
    ) -> Result<Self, BackendInitError> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        let commit_db = Commits::open(&base_path, Arc::clone(&object_store), rt.handle().clone())?;
        Ok(Self {
            base_path,
            object_store,
            commits: commit_db,
            rt,
        })
    }
}

struct Commits {
    commits: RefCell<Vec<SnapshotVersion>>,
    commits_path: Path,
    object_store: Arc<dyn ObjectStore>,
    rt: Handle,
}

impl Commits {
    /// Loads the commits from disk or an empty commit vector if no commits
    /// on disk exists
    fn open(
        base_path: &Path,
        object_store: Arc<dyn ObjectStore>,
        rt: Handle,
    ) -> Result<Self, OpenCommitsError> {
        let commits_path = base_path.child("commits");
        let file = rt.block_on(object_store.get(&commits_path));
        match file {
            Ok(x) => {
                let content = rt.block_on(x.bytes())?;
                let commits: Vec<SnapshotVersion> = rmp_serde::from_slice(&content.to_vec())?;
                Ok(Self {
                    commits: RefCell::new(commits),
                    commits_path,
                    object_store,
                    rt,
                })
            }
            Err(object_store::Error::NotFound { .. }) => Ok(Self {
                commits: Default::default(),
                commits_path,
                object_store,
                rt,
            }),
            Err(e) => Err(OpenCommitsError::ObjectStore(e)),
        }
    }

    /// Check if a specific version has been committed
    fn is_commited(&self, version: &SnapshotVersion) -> bool {
        self.commits.borrow().contains(version)
    }

    /// Get the last commited version or None if there have not been
    /// any commits
    fn get_last_commited(&self) -> Option<SnapshotVersion> {
        self.commits.borrow().last().cloned()
    }

    /// Commit a version to persistent storage
    fn commit(&self, version: SnapshotVersion) -> Result<(), CommitError> {
        // PANIC: I think this can not reasonably fail
        self.commits.borrow_mut().push(version);
        let encoded = rmp_serde::to_vec(&self.commits).expect("Encode vec");
        let payload = PutPayload::from(encoded);
        self.rt
            .block_on(self.object_store.put(&self.commits_path, payload))?;
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum OpenCommitsError {
    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),
    #[error("DecodingError: Commit file is corrupt or incompatible {0}")]
    Decoding(#[from] rmp_serde::decode::Error),
}

#[derive(Debug, Error)]
enum CommitError {
    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),
    #[error("DecodingError: Commit file is corrupt or incompatible {0}")]
    Decoding(#[from] rmp_serde::decode::Error),
}

/// Deletes an entire directory/prefix from the object store
async fn delete_dir<S: ObjectStore>(store: &S, prefix: &Path) -> Result<(), object_store::Error> {
    let objects = store.list(Some(prefix));
    let stream = objects.then(|x| delete(store, x));
    let mut stream = pin!(stream);
    while let Some(res) = stream.next().await {
        res?
    }
    Ok(())
}

async fn delete<S: ObjectStore>(
    store: &S,
    object: Result<object_store::ObjectMeta, object_store::Error>,
) -> Result<(), object_store::Error> {
    let path = &object?.location;
    store.delete(path).await
}

#[derive(Debug, Error)]
pub enum BackendInitError {
    #[error("Error starting Tokio runtime: {0}")]
    TokioRuntime(#[from] std::io::Error),
    #[error("Error opening commits")]
    OpenCommits(#[from] OpenCommitsError),
}

pub struct SlateDbClient {
    db: Db,
    version: SnapshotVersion,
    rt: Handle,
}

impl SlateDbClient {
    fn new(db: Db, version: SnapshotVersion, rt: Handle) -> Self {
        Self { db, version, rt }
    }
}

impl PersistenceClient for SlateDbClient {
    fn get_version(&self) -> SnapshotVersion {
        self.version
    }

    fn load(&self, operator_id: &crate::types::OperatorId) -> Option<Vec<u8>> {
        debug!("Restoring state for operator {}", operator_id);
        self.rt
            .block_on(self.db.get(&operator_id.to_be_bytes()))
            .unwrap()
            .map(|x| x.to_vec())
    }

    fn persist(&mut self, state: &[u8], operator_id: &crate::types::OperatorId) {
        debug!("Storing state for operator {}", operator_id);
        self.rt
            .block_on(self.db.put(&operator_id.to_be_bytes(), state))
            .unwrap()
    }
}

impl Drop for SlateDbClient {
    fn drop(&mut self) {
        self.rt.block_on(self.db.flush()).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use crate::{
        keyed::KeyLocal,
        operators::*,
        runtime::{threaded::SingleThreadRuntime, WorkerBuilder},
        sources::{SingleIteratorSource, StatelessSource},
        testing::VecSink,
    };

    use super::*;
    use object_store::{memory::InMemory, path::Path};

    /// check we return a client for version 0 if there has not been a committed
    /// version yet
    #[test]
    fn client_0_if_no_committed() {
        let store = InMemory::new();
        let backend = SlateDbBackend::new(Arc::new(store), Path::default()).unwrap();
        let client = backend.last_commited(11);
        assert_eq!(client.version, 0);
    }

    /// Check we return a client for the last committed version
    #[test]
    fn last_committed_client() {
        let store = InMemory::new();
        let backend = SlateDbBackend::new(Arc::new(store), Path::default()).unwrap();
        backend.commit_version(&42);
        let client = backend.last_commited(0);
        assert_eq!(client.version, 42);
    }

    /// Check we return a client for the last committed version, not the highest version
    #[test]
    fn last_committed_not_highest_client() {
        let store = InMemory::new();
        let backend = SlateDbBackend::new(Arc::new(store), Path::default()).unwrap();
        backend.commit_version(&42);
        backend.commit_version(&3);
        let client = backend.last_commited(0);
        assert_eq!(client.version, 3);
    }

    /// Check we return a client for the requested version
    #[test]
    fn for_specific_version() {
        let store = InMemory::new();
        let backend = SlateDbBackend::new(Arc::new(store), Path::default()).unwrap();
        let client = backend.for_version(0, &7);
        assert_eq!(client.version, 7);
    }

    /// Check we can store data with a client and then retrieve it again
    #[test]
    fn store_and_retrieve() {
        let store = InMemory::new();
        let backend = SlateDbBackend::new(Arc::new(store), Path::default()).unwrap();
        let mut client = backend.last_commited(0);
        let state = b"HelloWorld";
        client.persist(state, &1337);
        backend.commit_version(&client.get_version());

        let load_client = backend.last_commited(0);
        let restored = load_client.load(&1337).unwrap();
        assert_eq!(state, &restored[..])
    }

    /// Check we do not restore uncommitted changes
    #[test]
    fn no_uncommitted_restored() {
        let store = InMemory::new();
        let backend = SlateDbBackend::new(Arc::new(store), Path::default()).unwrap();
        let mut client = backend.last_commited(0);
        let state = b"HelloWorld";
        client.persist(state, &1337);

        let load_client = backend.last_commited(0);
        let restored = load_client.load(&1337);
        assert!(restored.is_none())
    }

    /// A full dataflow test
    #[test]
    fn full_test() {
        let fs = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let capture = VecSink::new();
        for _ in 0..2 {
            let backend = SlateDbBackend::new(Arc::clone(&fs), Path::from("/snapshots")).unwrap();
            let rt = SingleThreadRuntime::new(|flavor| {
                let mut worker = WorkerBuilder::new(flavor, || true, backend);
                worker
                    .new_stream()
                    .source(
                        "source",
                        StatelessSource::new(SingleIteratorSource::new(0..2)),
                    )
                    .key_local("key-local", |_| 0)
                    .stateful_map("sum", |_, val, state: i32| {
                        let sum = val + state;
                        (sum, Some(sum))
                    })
                    .sink("sink", capture.clone())
                    .finish();
                worker
            });
            rt.execute().unwrap();
        }
        let expected = vec![0, 1, 1, 2];
        let result: Vec<i32> = capture.into_iter().map(|x| x.value).collect();
        assert_eq!(result, expected);
    }
}
