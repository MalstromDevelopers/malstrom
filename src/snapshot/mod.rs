use crate::{
    channels::selective_broadcast::{Receiver, Sender},
    stream::jetstream::NoData,
};

use self::backend::{PersistenceBackend, NoPersistenceBackend};
pub mod backend;
pub mod barrier;

pub trait SnapshotController<P: PersistenceBackend> {
    /// this method is called by the worker to schedule the snapshot controller
    fn evaluate(&self) -> ();

    fn get_backend_mut(&mut self) -> &mut P;
}
pub struct SnapshotControllerBuilder<T, P> {
    roots: Sender<NoData>,
    leafs: Vec<Receiver<T>>,
    worker_id: usize,
    backend: P,
}

impl<T, P> SnapshotController<P> for SnapshotControllerBuilder<T, P>
where
    P: PersistenceBackend,
{
    fn evaluate(&self) {
        todo!()
    }

    /// Get the persistent backend backing snapshots
    fn get_backend_mut(&mut self) -> &mut P {
        &mut self.backend
    }
}

/// A Snapshot controller that never snapshots
#[derive(Default)]
pub struct NoSnapshotController(NoPersistenceBackend);

impl SnapshotController<NoPersistenceBackend> for NoSnapshotController {
    fn evaluate(&self) -> () {
        ()
    }

    fn get_backend_mut(&mut self) -> &mut NoPersistenceBackend {
        &mut self.0
    }
}
    
