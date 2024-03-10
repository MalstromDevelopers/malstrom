pub mod barrier;
pub mod controller;
use crate::OperatorId;

pub type SnapshotVersion = usize;

pub trait PersistenceBackend: Clone + 'static {
    fn new_latest() -> Self;
    fn new_for_epoch(snapshot_epoch: &SnapshotVersion) -> Self;
    fn get_epoch(&self) -> SnapshotVersion;
    fn load<S>(&self, operator_id: OperatorId) -> Option<S>;
    fn persist<S>(&mut self, state: &S, operator_id: OperatorId);
}

#[derive(Default, Clone, Debug)]
pub struct NoPersistenceBackend {
    epoch: SnapshotVersion,
}
impl PersistenceBackend for NoPersistenceBackend {
    fn new_latest() -> Self {
        NoPersistenceBackend::default()
    }

    fn new_for_epoch(snapshot_epoch: &SnapshotVersion) -> Self {
        NoPersistenceBackend {
            epoch: *snapshot_epoch,
        }
    }

    fn get_epoch(&self) -> SnapshotVersion {
        self.epoch
    }

    fn load<S>(&self, _operator_id: OperatorId) -> Option<S> {
        None
    }

    fn persist<S>(&mut self, _state: &S, _operator_id: OperatorId) {}
}
