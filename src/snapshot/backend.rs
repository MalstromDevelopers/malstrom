use crate::frontier::Timestamp;

pub trait State: Default + Clone + 'static {}
impl<T: Default + Clone + 'static> State for T {}

/// A Marker type to use in stateless operators
#[derive(Default, Clone)]
pub struct NoState;

pub trait PersistenceBackend<P: PersistentState>: 'static {
    fn get(&self, state_id: usize, snapshot_epoch: usize) -> P;
}

pub trait PersistentState: 'static {
    fn load_frontier(&self) -> Timestamp;
    fn load<S>(&self) -> Option<S>;
    fn persist<S>(&self, frontier: Timestamp, state: &S, snapshot_epoch: usize) -> ();
}

/// A backend which does not persist anything
#[derive(Default)]
pub struct NoPersistenceBackend{}

impl PersistentState for NoPersistenceBackend {

    fn load_frontier(&self) -> Timestamp {
        Timestamp::default()
    }

    fn load<S>(&self) -> Option<S> {
        None
    }

    fn persist<S>(&self, frontier: Timestamp, state: &S, snapshot_epoch: usize) -> () {
        ()
    }
}