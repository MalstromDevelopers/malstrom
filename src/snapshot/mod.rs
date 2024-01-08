pub mod barrier;
pub mod configmap;
mod server;
use crate::{frontier::Timestamp, channels::selective_broadcast::{Sender, Receiver}, stream::jetstream::NoData};

pub trait SnapshotController<P: PersistenceBackend> {
    fn setup(&mut self, roots: Vec<Sender<NoData, P>>, leafs: Vec<Receiver<NoData, P>>) -> ();

    /// this method is called by the worker to schedule the snapshot controller
    fn evaluate(&mut self) -> ();
    
    /// if this method is called the SnapshotController should send
    /// a load state instruction along the dataflow channels
    fn load_state(&mut self) -> ();
}
/// A Snapshot controller that never snapshots
#[derive(Default)]
pub struct NoSnapshotController;

impl<P> SnapshotController<P> for NoSnapshotController where P: PersistenceBackend{

    fn setup(&mut self, _roots: Vec<Sender<NoData, P>>, _leafs: Vec<Receiver<NoData, P>>) -> () {
       () 
    }

    fn evaluate(&mut self) -> () {
        ()
    }

    fn load_state(&mut self) -> () {
        ()
    }
}
pub trait PersistenceBackend: Clone + 'static {
    fn load<S>(&self, operator_id: usize) -> (Timestamp, S);
    fn persist<S>(&mut self, frontier: Timestamp, state: &S, operator_id: usize) -> ();
}
