use std::ops::AddAssign;
use std::sync::{Arc, RwLock};

/// Module for persistent, stateful operators for timely dataflow
/// 
/// Any stateful operator is composed of four operators
/// 
/// 1. factory: Loads or creates the initial state
/// 2. logic: arbitrary logic which manipulates state or data
/// 3. timer: arbitrary logic which decides whether persistance should be triggered
/// 4: persistor: logic which saves the state to a persistent storage

use timely::Data;
use timely::dataflow::operators::{Map, Probe};
use timely::dataflow::{Stream, Scope, InputHandle};
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::ScopeParent;
use serde::de::DeserializeOwned;
use serde::Serialize;
pub trait PersistentState: Data + Serialize + DeserializeOwned {}
impl<T: Data + Serialize + DeserializeOwned> PersistentState for T {}


pub fn shared_counter() -> (usize, impl Fn() -> ()) {
    let counter: usize = 0;
    let func = move || {if &counter < &20 {println!("Smaller than 20")};};

    (counter, func)
}

pub fn use_shared_counter() {
    let (mut offset, func) = shared_counter();
    offset += 1;

    func();
}

/// Extension trait for `Stream`.
pub trait PersistentMap<S: Scope, D: Data, P: PersistentState> {
    /// Consumes each element 
    fn map<
    D2: Data,
    P2: PersistentState, 
    // TODO: Timestamp hardcoded to usize
    StateBackend: PersistentStateBackend<usize, P>,
    Logic: FnMut(D, P)->(D2, P2) + 'static,
    >(&self, state_backend: StateBackend, logic: Logic) -> Stream<S, D2>;
}

/// Interface for a backend to persist state
/// 
/// This could really be anything. The simplest case
/// is a local filesystem, where state is saved
/// but could also be an object store or a database
pub trait PersistentStateBackend<T, P: PersistentState> {
    type LoadError: std::error::Error;
    type SaveError: std::error::Error;

    /// Load the latest state snapshot for the given worker.
    /// 
    /// Returns either the state or an error
    /// if no state was found or it could not be loaded
    fn load_state_latest(&self, worker_index: usize) -> Result<P, Self::LoadError>;

    /// Save a state snapshot for the given worker and frontier
    /// 
    /// Return an error if the state could not be saved
    /// 
    /// This function is absolutely free to do nothing, if the state does not need
    /// to be saved at the time of calling
    /// 
    /// TODO: expose timely's progress to this method
    fn save_state(&self, worker_index: usize, state: &P) -> Result<(), Self::SaveError>;

}

/// Create a new stream from a persisted stateful source
/// 
/// This works by supplying a timely worker's scope
/// 
/// A factory to create the initial state (if it is not on disk)
/// A source which generates records
pub fn persistent_source<P: PersistentState, S: Scope, D: Data>(
    scope: &mut S,
    state_backend: impl PersistentStateBackend<usize, P, LoadError = impl std::error::Error, SaveError = impl std::error::Error> + 'static,
    source: impl FnMut(P) -> (D, P) + 'static,
) -> Stream<S, D>
where
    S: ScopeParent<Timestamp = usize>
    {
    let initial_state = state_backend.load_state_latest(scope.index()).expect("Error initializing state");
    // TODO we hardcode the timestamp type to usize here
    // but that may no be desirable to users
    let state_input = Arc::new(RwLock::new(InputHandle::<usize, P>::new()));
    let state_input_inner = state_input.clone();

    let state_stream = state_input.write().unwrap().to_stream(scope);
    let worker_index = scope.index();
    
    // call the given function to generate input then
    // disentangle the state and data. Feed the state into the state_input
    // and the data downstream
    let data_stream = state_stream
        .map(source)
        .map_in_place(move |(_, state)| {
            // TODO, do something better than expect here
            state_backend.save_state(worker_index, &state).expect("Error saving state");
        })
        // destructure
        .map(move |(data, state)| {
            // feed the state back into the input
            let mut state_input_handle = state_input_inner.write().unwrap();
            let time = state_input_handle.time().clone();
            state_input_handle.send(state);
            state_input_handle.advance_to(time + 1);
            data
        });
    
    // Not sure why, but for some reason it is really important this happens
    // after calling all the map stuff above
    // load or create the initial state
    state_input.write().unwrap().send(initial_state);
    state_input.write().unwrap().advance_to(1);
    data_stream
}