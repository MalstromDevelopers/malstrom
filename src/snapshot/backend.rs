use crate::frontier::Timestamp;

pub trait State: Default + Clone + 'static {}
impl<T: Default + Clone + 'static> State for T {}

/// A Marker type to use in stateless operators
#[derive(Default, Clone)]
pub struct NoState;

pub trait PersistenceBackend {
    fn load_frontier(&self, state_id: usize) -> Timestamp;
    fn load<S>(&self, state_id: usize) -> Option<S>;
    fn persist<S>(&self, state_id: usize, frontier: Timestamp, state: &S, snapshot_epoch: usize) -> ();
}

/// A backend which does not persist anything
#[derive(Default)]
pub struct NoPersistenceBackend{}

impl PersistenceBackend for NoPersistenceBackend {
    fn load_frontier(&self, state_id: usize) -> Timestamp {
        Timestamp::default()
    }

    fn load<S>(&self, state_id: usize) -> Option<S> {
        None
    }

    fn persist<S>(&self, state_id: usize, frontier: Timestamp, state: &S, snapshot_epoch: usize) -> () {
        ()
    }
}