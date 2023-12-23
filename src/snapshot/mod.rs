use crate::{
    channels::selective_broadcast::{Receiver, Sender},
    stream::jetstream::NoData,
};

use self::backend::{PersistentState, NoPersistenceBackend};
pub mod backend;
pub mod barrier;

pub trait SnapshotController {
    /// this method is called by the worker to schedule the snapshot controller
    fn evaluate(&self) -> ();

    /// get the number of the latest usable snapshot
    fn get_latest(&self) -> usize;
}
/// A Snapshot controller that never snapshots
#[derive(Default)]
pub struct NoSnapshotController;

impl SnapshotController for NoSnapshotController {
    fn evaluate(&self) -> () {
        ()
    }
}
    
