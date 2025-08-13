//! Snapshots are periodically saved state from stateful operations. Regular snapshots allow
//! resuming computation after failures. Snapshots can also be utilized to enable statful job
//! upgrades

#[cfg(feature = "slatedb")]
pub mod slatedb;
use crate::types::{OperatorId, WorkerId};
use serde::{de::DeserializeOwned, Serialize};
#[cfg(feature = "slatedb")]
pub use slatedb::{object_store, SlateDbBackend, SlateDbClient};
use std::{fmt::Debug, rc::Rc, sync::Mutex};

/// Version of a snapshot
pub type SnapshotVersion = u64;

pub(crate) fn serialize_state<S: Serialize>(state: &S) -> Vec<u8> {
    rmp_serde::to_vec(state).expect("Error serializing state")
}

pub(crate) fn deserialize_state<S: DeserializeOwned>(state: Vec<u8>) -> S {
    rmp_serde::from_slice(&state).expect("Error deserializing state")
}

/// A persistence backend provides persistent storage for storing snapshots across job restarts.
/// This may be on a local disk, remote storage, a database or anything really which can reliably
/// store data
pub trait PersistenceBackend: 'static {
    /// Client for this backend. The client is used to store and load state from the backend.
    type Client: PersistenceClient;
    /// Return the version of the last committed snapshot or `None` if no version has not been
    /// committed yet.
    fn last_commited(&self) -> Option<SnapshotVersion>;
    /// Create a client for a loading/saving state for a specific snapshot version
    fn for_version(&self, worker_id: WorkerId, snapshot_version: &SnapshotVersion) -> Self::Client;
    /// mark a specific snapshot version as finished
    fn commit_version(&self, snapshot_version: &SnapshotVersion);
}

/// A client for saving snapshot data to and loading that data from a persistent storage
pub trait PersistenceClient: 'static {
    /// Load the state for the given operator, returning `None` if no state exists for this
    /// operator in persistent storage
    fn load(&self, operator_id: &OperatorId) -> Option<Vec<u8>>;
    /// Retain the given state for the given operator.
    fn persist(&mut self, state: &[u8], operator_id: &OperatorId);
}

/// A snapshotting barrier for use with the
/// [ABS snapshotting algorithm](https://arxiv.org/abs/1506.08603)
pub struct Barrier {
    backend: Rc<Mutex<Box<dyn PersistenceClient>>>,
}
impl Clone for Barrier {
    fn clone(&self) -> Self {
        Self {
            backend: Rc::clone(&self.backend),
        }
    }
}
impl Debug for Barrier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Barrier").finish()
    }
}

impl Barrier {
    pub(super) fn new(backend: Box<dyn PersistenceClient>) -> Self {
        Self {
            backend: Rc::new(Mutex::new(backend)),
        }
    }

    /// Persist the given state for the given operator.
    pub fn persist<S: Serialize + DeserializeOwned>(
        &mut self,
        state: &S,
        operator_id: &OperatorId,
    ) {
        let encoded = serialize_state(state);
        #[allow(clippy::unwrap_used)]
        self.backend.lock().unwrap().persist(&encoded, operator_id)
    }

    pub(super) fn strong_count(&self) -> usize {
        Rc::strong_count(&self.backend)
    }
}

/// A persistence backend which does not retain any data. This is mostly useful for testing or
/// situations where you always want to restart the job statelessly
#[derive(Clone, Debug)]
pub struct NoPersistence;
impl PersistenceBackend for NoPersistence {
    type Client = NoPersistence;
    fn last_commited(&self) -> Option<SnapshotVersion> {
        None
    }

    fn for_version(&self, _worker_id: WorkerId, _snapshot_version: &SnapshotVersion) -> Self {
        NoPersistence {}
    }

    fn commit_version(&self, _snapshot_version: &SnapshotVersion) {}
}

impl PersistenceClient for NoPersistence {
    fn load(&self, _operator_id: &OperatorId) -> Option<Vec<u8>> {
        None
    }

    fn persist(&mut self, _state: &[u8], _operator_id: &OperatorId) {}

    // fn commit(&mut self, _snapshot_epoch: &SnapshotVersion) -> () {
    //     ()
    // }

    // fn get_last_committed(&self) -> Option<SnapshotVersion> {
    //     None
    // }
}

#[cfg(test)]
mod test {
    use super::PersistenceClient;

    /// This test won't compile if PersistenceBackend is not object safe
    #[test]
    fn is_object_safe() {
        struct _Foo {
            _bar: Box<dyn PersistenceClient>,
        }
    }
}
