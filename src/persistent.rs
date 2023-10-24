use serde::de::DeserializeOwned;
use serde::Serialize;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::operators::{ConnectLoop, Enter, Feedback, Leave};
use timely::dataflow::ScopeParent;
use timely::dataflow::{Scope, Stream};
use timely::order::TotalOrder;
use timely::progress::timestamp::Refines;
use timely::progress::Timestamp;
use timely::Data;
pub trait PersistentState: Data + Serialize + DeserializeOwned {}
impl<T: Data + Serialize + DeserializeOwned> PersistentState for T {}

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

use timely::dataflow::operators::generic::operator;

use crate::split::Split;

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
            if let (Some(cap), Some(val)) = (cap.as_mut(), val_opt.take()) {
                output.session(&cap).give(val);
            }
            cap = None;
        }
    })
}

type StateTimestamp = usize;
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

    // This is a stream, which will only provide one unit
    // type to start the dataflow
    let init_state_stream = once(scope, ());
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
                |_default_cap, _info| {
                    move |initial_state_input, loopback_input, output| {
                        let mut state_vec = Vec::new();

                        // feed the initial state into the dataflow, this should
                        // only happen once
                        while let Some((time, _)) = initial_state_input.next() {
                            let new_time = time.delayed(&(time.time() + 1));
                            let initial_state =
                                state_backend.load_state_latest(worker_index).expect(
                                    "
                            Failed to load state from backend
                            ",
                                );
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
}
