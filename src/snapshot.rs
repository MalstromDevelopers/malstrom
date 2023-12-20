use crate::channels::selective_broadcast::{Receiver, Sender};
pub struct SnapshotController<T> {
    roots: Sender<T>,
    leafs: Vec<Receiver<T>>,
    worker_id: usize
}

impl<T> SnapshotController<T> {

    /// The SnapshotController gets instantiated with a Sender
    /// attached to the dataflow tree roots and receivers attached
    /// to the dataflow tree leafs
    /// The worker_id is this local workers id
    pub fn new(roots: Sender<T>, leafs: Vec<Receiver<T>>, worker_id: usize) {
        todo!()
    }

    /// this method is called by the worker to schedule the snapshot controller
    pub fn step() {
        todo!()
    }
}