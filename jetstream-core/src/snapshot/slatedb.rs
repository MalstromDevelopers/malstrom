use std::{pin::pin, sync::Arc};

use super::{PersistenceBackend, PersistenceClient, SnapshotVersion};
use crate::types::WorkerId;
use bytes::Buf;
use object_store::{path::Path, ObjectStore};
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
    commit_db: Db,
    rt: Runtime,
}

impl PersistenceBackend for SlateDbBackend {
    type Client = SlateDbClient;

    fn last_commited(&self, worker_id: WorkerId) -> Self::Client {
        let last_commit = self
            .rt
            .block_on(get_latest_committed(&self.commit_db))
            .expect("Can get latest commit")
            .unwrap_or(0);
        self.for_version(worker_id, &last_commit)
    }

    fn for_version(&self, worker_id: WorkerId, snapshot_version: &SnapshotVersion) -> Self::Client {
        let version_is_committed = self
            .rt
            .block_on(check_if_committed(&self.commit_db, snapshot_version))
            .expect("Expect to get commit information");

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
        SlateDbClient::new(snapshot_db, *snapshot_version, self.rt.handle().to_owned())
    }

    fn commit_version(&self, snapshot_version: &SnapshotVersion) {
        self.rt
            .block_on(commit_version(&self.commit_db, snapshot_version))
            .unwrap()
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
        let commit_db_open = Db::open(base_path.child("commits"), Arc::clone(&object_store));
        let commit_db = rt.block_on(commit_db_open)?;
        Ok(Self {
            base_path,
            object_store,
            commit_db,
            rt,
        })
    }
}

/// get the last committed version or none if no version has been
/// committed yet
async fn get_latest_committed(
    commit_db: &Db,
) -> Result<Option<SnapshotVersion>, GetCommitedVersionsError> {
    let latest = commit_db.get(b"latest").await?;
    match latest {
        Some(mut x) => {
            if x.len() != 8 {
                Err(GetCommitedVersionsError::CorruptKey(x.len()))
            } else {
                Ok(Some(x.get_u64()))
            }
        }
        None => Ok(None),
    }
}

/// commit a new snapshot version
async fn commit_version(commit_db: &Db, version: &SnapshotVersion) -> Result<(), SlateDBError> {
    commit_db.put(b"latest", &version.to_be_bytes()).await?;
    commit_db.put(&version.to_be_bytes(), &[]).await?;
    commit_db.flush().await
}

/// Check if the given version was committed
async fn check_if_committed(
    commit_db: &Db,
    version: &SnapshotVersion,
) -> Result<bool, SlateDBError> {
    commit_db
        .get(&version.to_be_bytes())
        .await
        .map(|x| x.is_some())
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
    #[error("Error creating commit database")]
    CreateCommitDb(#[from] SlateDBError),
}

#[derive(Debug, Error)]
pub enum GetCommitedVersionsError {
    #[error("Error retrieving latest commit: {0}")]
    RetrieveLatest(#[from] SlateDBError),
    #[error("Latest Commit key is corrupted. Unexpected length (expected 8): {0}")]
    CorruptKey(usize),
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
    use std::sync::Arc;

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
}
