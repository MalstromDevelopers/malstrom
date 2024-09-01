pub mod controller;
use std::{fmt::Debug, rc::Rc, sync::Mutex};

use serde::{de::DeserializeOwned, Serialize};

use crate::types::{OperatorId, WorkerId};

pub type SnapshotVersion = usize;

const BINCODE_CONFIG: bincode::config::Configuration = bincode::config::standard();

pub(crate) fn serialize_state<S: Serialize>(state: &S) -> Vec<u8> {
    bincode::serde::encode_to_vec(state, BINCODE_CONFIG).expect("Error serializing state")
}

pub(crate) fn deserialize_state<S: DeserializeOwned>(state: Vec<u8>) -> S {
    bincode::serde::decode_from_slice(&state, BINCODE_CONFIG)
        .expect("Error deserializing state")
        .0
}

pub trait PersistenceBackend: 'static {
    fn latest(&self, worker_id: WorkerId) -> Box<dyn PersistenceClient>;
    fn for_version(
        &self,
        worker_id: WorkerId,
        snapshot_epoch: &SnapshotVersion,
    ) -> Box<dyn PersistenceClient>;
}

pub trait PersistenceClient: Debug {
    fn get_version(&self) -> SnapshotVersion;
    fn load(&self, operator_id: &OperatorId) -> Option<Vec<u8>>;
    fn persist(&mut self, state: &[u8], operator_id: &OperatorId);
}

#[derive(Debug)]
pub struct Barrier {
    backend: Rc<Mutex<Box<dyn PersistenceClient>>>,
}
impl Clone for Barrier {
    fn clone(&self) -> Self {
        Self {
            backend: self.backend.clone(),
        }
    }
}

impl Barrier {
    pub(super) fn new(backend: Box<dyn PersistenceClient>) -> Self {
        Self {
            backend: Rc::new(Mutex::new(backend)),
        }
    }

    pub fn persist<S: Serialize + DeserializeOwned>(
        &mut self,
        state: &S,
        operator_id: &OperatorId,
    ) {
        let encoded = serialize_state(state);
        self.backend.lock().unwrap().persist(&encoded, operator_id)
    }

    pub(super) fn strong_count(&self) -> usize {
        Rc::strong_count(&self.backend)
    }

    pub(super) fn get_version(&self) -> SnapshotVersion {
        self.backend.lock().unwrap().get_version()
    }
}

#[derive(Default, Clone, Debug)]
pub struct NoPersistence {
    epoch: SnapshotVersion,
}
impl PersistenceBackend for NoPersistence {
    fn latest(&self, _worker_id: WorkerId) -> Box<dyn PersistenceClient> {
        Box::new(self.clone())
    }

    fn for_version(
        &self,
        _worker_id: WorkerId,
        snapshot_epoch: &SnapshotVersion,
    ) -> Box<dyn PersistenceClient> {
        Box::new(NoPersistence {
            epoch: *snapshot_epoch,
        })
    }
}

impl PersistenceClient for NoPersistence {
    fn get_version(&self) -> SnapshotVersion {
        self.epoch
    }

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
