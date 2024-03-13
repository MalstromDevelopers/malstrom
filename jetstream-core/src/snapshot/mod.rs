pub mod controller;
use std::{rc::Rc, sync::Mutex};

use crate::{OperatorId, WorkerId};

pub type SnapshotVersion = usize;

pub trait PersistenceBackend: 'static {
    fn new_latest(worker_id: WorkerId) -> Self;
    fn new_for_version(worker_id: WorkerId, snapshot_epoch: &SnapshotVersion) -> Self;
    fn get_version(&self) -> SnapshotVersion;
    fn load<S>(&self, operator_id: OperatorId) -> Option<S>;
    fn persist<S>(&mut self, state: &S, operator_id: OperatorId);

    // fn commit(&mut self, snapshot_epoch: &SnapshotVersion) -> ();
    // fn get_last_committed(&self) -> Option<SnapshotVersion>;
}

#[derive(Debug)]
pub struct Barrier<P> {
    backend: Rc<Mutex<P>>,
}
impl<P> Clone for Barrier<P> {
    fn clone(&self) -> Self {
        Self {
            backend: self.backend.clone(),
        }
    }
}

impl<P> Barrier<P>
where
    P: PersistenceBackend,
{
    pub(super) fn new(backend: P) -> Self {
        Self {
            backend: Rc::new(Mutex::new(backend)),
        }
    }

    pub fn persist<S>(&mut self, state: &S, operator_id: OperatorId) {
        self.backend.lock().unwrap().persist(state, operator_id)
    }

    pub(super) fn strong_count(&self) -> usize {
        Rc::strong_count(&self.backend)
    }

    pub(super) fn get_version(&self) -> SnapshotVersion {
        self.backend.lock().unwrap().get_version()
    }
}
#[derive(Debug)]
pub struct Load<P> {
    backend: Rc<Mutex<P>>,
}
impl<P> Clone for Load<P> {
    fn clone(&self) -> Self {
        Self {
            backend: self.backend.clone(),
        }
    }
}

impl<P> Load<P>
where
    P: PersistenceBackend,
{
    pub(super) fn new(backend: P) -> Self {
        Self {
            backend: Rc::new(Mutex::new(backend)),
        }
    }
    pub fn load<S>(&self, operator_id: OperatorId) -> Option<S> {
        self.backend.lock().unwrap().load(operator_id)
    }
}

#[derive(Default, Clone, Debug)]
pub struct NoPersistence {
    epoch: SnapshotVersion,
}
impl PersistenceBackend for NoPersistence {
    fn new_latest(_worker_id: WorkerId) -> Self {
        NoPersistence::default()
    }

    fn new_for_version(_worker_id: WorkerId, snapshot_epoch: &SnapshotVersion) -> Self {
        NoPersistence {
            epoch: *snapshot_epoch,
        }
    }

    fn get_version(&self) -> SnapshotVersion {
        self.epoch
    }

    fn load<S>(&self, _operator_id: OperatorId) -> Option<S> {
        None
    }

    fn persist<S>(&mut self, _state: &S, _operator_id: OperatorId) {}

    // fn commit(&mut self, _snapshot_epoch: &SnapshotVersion) -> () {
    //     ()
    // }

    // fn get_last_committed(&self) -> Option<SnapshotVersion> {
    //     None
    // }
}
