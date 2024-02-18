pub mod barrier;
pub mod controller;
use crate::{frontier::Timestamp, OperatorId};

pub trait PersistenceBackend: Clone + 'static {
    fn new_latest() -> Self;
    fn new_for_epoch(snapshot_epoch: &u64) -> Self;
    fn get_epoch(&self) -> u64;
    fn load<S>(&self, operator_id: OperatorId) -> Option<(Timestamp, S)>;
    fn persist<S>(&mut self, frontier: Timestamp, state: &S, operator_id: OperatorId);
}

#[derive(Default, Clone)]
pub struct NoPersistenceBackend {
    epoch: u64,
}
impl PersistenceBackend for NoPersistenceBackend {
    fn new_latest() -> Self {
        NoPersistenceBackend::default()
    }

    fn new_for_epoch(snapshot_epoch: &u64) -> Self {
        NoPersistenceBackend {
            epoch: *snapshot_epoch,
        }
    }

    fn get_epoch(&self) -> u64 {
        self.epoch
    }

    fn load<S>(&self, _operator_id: OperatorId) -> Option<(Timestamp, S)> {
        None
    }

    fn persist<S>(&mut self, _frontier: Timestamp, _state: &S, _operator_id: OperatorId) {}
}
