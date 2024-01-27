pub mod barrier;
pub mod controller;
mod server;
use crate::{
    channels::selective_broadcast::{Receiver, Sender},
    frontier::Timestamp,
    stream::jetstream::NoData,
    OperatorId,
};

pub trait SnapshotController<P: PersistenceBackend> {
    fn setup(&mut self, roots: Vec<Sender<NoData, P>>, leafs: Vec<Receiver<NoData, P>>);

    /// this method is called by the worker to schedule the snapshot controller
    fn evaluate(&mut self);

    /// if this method is called the SnapshotController should send
    /// a load state instruction along the dataflow channels
    fn load_state(&mut self);
}
/// A Snapshot controller that never snapshots
#[derive(Default)]
pub struct NoSnapshotController;

impl<P> SnapshotController<P> for NoSnapshotController
where
    P: PersistenceBackend,
{
    fn setup(&mut self, _roots: Vec<Sender<NoData, P>>, _leafs: Vec<Receiver<NoData, P>>) {}

    fn evaluate(&mut self) {}

    fn load_state(&mut self) {}
}
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
