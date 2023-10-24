use std::ops::AddAssign;
use std::sync::{Arc, RwLock};

use serde::de::DeserializeOwned;
use serde::Serialize;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::operator::{empty, Operator};
use timely::dataflow::operators::{
    ConnectLoop, Enter, Feedback, Inspect, Leave, Map, Partition, Probe, ToStream,
};
use timely::dataflow::ScopeParent;
use timely::dataflow::{InputHandle, Scope, Stream};
use timely::order::TotalOrder;
use timely::progress::timestamp::Refines;
use timely::progress::Timestamp;
/// Module for persistent, stateful operators for timely dataflow
///
/// Any stateful operator is composed of four operators
///
/// 1. factory: Loads or creates the initial state
/// 2. logic: arbitrary logic which manipulates state or data
/// 3. timer: arbitrary logic which decides whether persistance should be triggered
/// 4: persistor: logic which saves the state to a persistent storage
use timely::Data;
pub trait PersistentState: Data + Serialize + DeserializeOwned {}
impl<T: Data + Serialize + DeserializeOwned> PersistentState for T {}

pub fn shared_counter() -> (usize, impl Fn() -> ()) {
    let counter: usize = 0;
    let func = move || {
        if &counter < &20 {
            println!("Smaller than 20")
        };
    };

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
        Logic: FnMut(D, P) -> (D2, P2) + 'static,
    >(
        &self,
        state_backend: StateBackend,
        logic: Logic,
    ) -> Stream<S, D2>;
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

fn make_state_provider() {}
use timely::dataflow::operators::generic::operator;
use timely::scheduling::Scheduler;

use crate::split::Split;

type StateTimestamp = usize;

/// provide one value to a stream and then never ever
/// do anything again
/// TODO: put this in a util package
fn once<G: Scope, D: Data>(scope: &G, value: D) -> Stream<G, D> {
    operator::source(scope, "Once", |capability, _info| {
        let mut cap = Some(capability);

        // we need to do a bit of a dance here with option
        // since technically our closure could be called more than once
        // but should that happen, we will just not output anything
        // ... it shouldn't because we drop the capability
        let mut val_opt = Some(value);
        move |output| {
            if let (Some(cap), Some(val)) = (cap.as_mut(), val_opt.take()){
                output.session(&cap).give(val);
            }
            cap = None;
        }
    })
}

/// Create a new stream from a persisted stateful source
///
/// This works by supplying a timely worker's scope
///
/// A factory to create the initial state (if it is not on disk)
/// A source which generates records
pub fn persistent_source<P: PersistentState, S: Scope, D: Data, T: Timestamp>(
    scope: &mut S,
    state_backend: impl PersistentStateBackend<
            T,
            P,
            LoadError = impl std::error::Error,
            SaveError = impl std::error::Error,
        > + 'static,
    mut source_func: impl FnMut(P) -> (D, P) + 'static,
) -> Stream<S, D>
where
    S: ScopeParent<Timestamp = T>,
    T: TotalOrder,
    StateTimestamp: Refines<T>,
{
    let worker_index = scope.index();

    // This is a stream, which will only provide one element, the initial state
    // let init_state_stream = operator::source<(scope, "StateInitializer", |capability, info| {
    //     let mut cap = Some(capability);
    //     move |output| {
    //         if let Some(cap) = cap.as_mut() {
    //             // get some data and send it.
    //             // let initial_state = state_backend
    //             //     .load_state_latest(worker_index)
    //             //     .expect("Error initializing state");
    //             output.session(&cap).give(None);
    //         }
    //         // exit after providing state once
    //         cap = None
    //     }
    // });
    let init_state_stream = once(
        scope,
        ()
    );
    scope.scoped::<StateTimestamp, _, _>("PersistentStateScope", move |persistent_scope| {
        // state loopback which feeds our state back to the
        // state provider, after the user modified it
        let (state_loop_handle, state_loop_cycle) = persistent_scope.feedback(1);

        let stream = init_state_stream
            .enter(persistent_scope)
            .binary(
                &state_loop_cycle,
                Pipeline,
                Pipeline,
                "StateProvider",
                |default_cap, _info| {
                    move |initial_state_input, loopback_input, output| {
                        let mut state_vec = Vec::new();
                        
                        // feed the initial state into the dataflow, this should
                        // only happen once
                        while let Some((time, _)) = initial_state_input.next() {
                            let new_time = time.delayed(&(time.time() + 1));
                            let initial_state = state_backend.load_state_latest(worker_index).expect("
                            Failed to load state from backend
                            ");
                            output.session(&new_time).give(initial_state);

                        }

                        // state loopback, this will be called everytime the state
                        // gets updated
                        while let Some((time, state)) = loopback_input.next() {
                            let new_time = time.delayed(&(time.time() + 1));
                            state.swap(&mut state_vec);
                            for s in state_vec.iter() {

                                // TODO: figure out a better way to handle state save failures
                                state_backend.save_state(worker_index, s).unwrap();
                            }

                            output.session(&new_time).give_vec(&mut state_vec);
                        }
                    }
                },
            )
            // execute the user function to actually produce some data
            .unary(Pipeline, "UserInputFunction", |_, _info| {
                move |input, output| {
                    while let Some((time, state)) = input.next() {
                        // TODO: this is really ugly
                        let mut state_vec = Vec::new();
                        state.swap(&mut state_vec);

                        let mut output_vec = Vec::with_capacity(state_vec.len());
                        for (i, s) in state_vec.into_iter().enumerate() {
                            let (data, new_state) = source_func(s);

                            // insert rather than push to preserve order
                            output_vec.insert(i, (data, new_state));
                        }
                        output.session(&time).give_vec(&mut output_vec);
                    }
                }
            });

        // split data and state
        let (data_stream, state_stream) = stream.split(|(data, state)| (data, state));
        // feed state back
        state_stream.connect_loop(state_loop_handle);
        data_stream.leave()
    })

    // let state_input = Arc::new(RwLock::new(InputHandle::<usize, P>::new()));
    // let state_input_inner = state_input.clone();
    // let worker_index = scope.index();
    // let state_stream = state_input.write().unwrap().to_stream(scope);

    // let persistent_region = scope.scoped::<usize, _, _>("PersistentSourceRegion", |subscope| {
    //     state_stream
    //         .enter(subscope)
    //         .map(source_func)
    //         .map_in_place(move |(_, state)| {
    //             // TODO, do something better than expect here
    //             state_backend
    //                 .save_state(worker_index, &state)
    //                 .expect("Error saving state");
    //         })
    //         // destructure
    //         .map(move |(data, state)| {
    //             // feed the state back into the input
    //             let mut state_input_handle = state_input_inner.write().unwrap();
    //             let time = state_input_handle.time().clone();
    //             state_input_handle.send(state);
    //             state_input_handle.advance_to(time + 1);
    //             data
    //         })
    //         .leave()
    // });

    // // call the given function to generate input then
    // // disentangle the state and data. Feed the state into the state_input
    // // and the data downstream
    // let data_stream = state_stream
    //     .map(source_func)
    //     .map_in_place(move |(_, state)| {
    //         // TODO, do something better than expect here
    //         state_backend
    //             .save_state(worker_index, &state)
    //             .expect("Error saving state");
    //     })
    //     // destructure
    //     .map(move |(data, state)| {
    //         // feed the state back into the input
    //         let mut state_input_handle = state_input_inner.write().unwrap();
    //         let time = state_input_handle.time().clone();
    //         state_input_handle.send(state);
    //         state_input_handle.advance_to(time + 1);
    //         data
    //     });

    // // Not sure why, but for some reason it is really important this happens
    // // after calling all the map stuff above
    // // load or create the initial state
    // state_input.write().unwrap().send(initial_state);
    // state_input.write().unwrap().advance_to(1);
    // data_stream
}
