use serde::de::DeserializeOwned;
use serde::Serialize;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::operators::{ConnectLoop, Enter, Feedback, Leave};
use timely::dataflow::Scope;
use timely::progress::Timestamp;
use timely::Data;

use crate::split::Split;
use crate::{JetStream, Empty, DataUnion};

// A datatype which is suitable to use as state for a stateful computation
/// in JetStream and can be persisted to endure system failures
///
/// Commonly used:
/// - Hashmaps
/// - B-Tree-Maps
/// - counters
pub trait PersistentState: Data + Default + Serialize + DeserializeOwned {}
impl<T: Data + Default + Serialize + DeserializeOwned> PersistentState for T {}
/// Interface for a backend to persist state
///
/// This could really be anything. The simplest case
/// is a local filesystem, where state is saved
/// but could also be an object store or a database
pub trait PersistentStateBackend<T, P: PersistentState> {
    /// Load the latest state snapshot for the given worker.
    ///
    /// Returns either the state or an error
    /// if no state was found or it could not be loaded
    fn load_state_latest(&self, worker_index: usize) -> Option<P>;

    /// Save a state snapshot for the given worker and frontier
    ///
    /// If an error occurs while saving, the function is expected
    /// to either handle it internally or panic.
    ///
    /// This function is absolutely free to do nothing, if the state does not need
    /// to be saved at the time of calling
    ///
    /// TODO: expose timely's progress to this method
    fn save_state(&self, worker_index: usize, state: &P) -> ();
}

pub trait PersistentInput<'scp, P: PersistentState, T: Timestamp, D1: Data, D2: Data> {
    /// A persistent input, which can be added to any stream
    /// The result is a stream of both the previous stream
    /// data type and the new stream data type
    fn persistent_input(
        self: Self,
        state_backend: impl PersistentStateBackend<
                T,
                P
            > + 'static,
        source_func: impl FnMut(P) -> (D2, P) + 'static,
    ) -> JetStream<'scp, DataUnion<D1, D2>>;
}
pub trait PersistentSource<'scp, P: PersistentState, T: Timestamp, D: Data> {
    /// A persistent source, which can be added to a stream, but
    /// completely discards any upstream data
    /// This is useful as the initial source of data of a stream
    fn persistent_source(
        self: Self,
        state_backend: impl PersistentStateBackend<
                T,
                P
            > + 'static,
        source_func: impl FnMut(P) -> (D, P) + 'static,
    ) -> JetStream<'scp, D>;
}

impl <'scp, P: PersistentState, T: Timestamp, D: Data> PersistentSource<'scp, P, T, D> for JetStream<'scp, Empty> {
    fn persistent_source(
        self: Self,
        state_backend: impl PersistentStateBackend<
                T,
                P
            > + 'static,
        mut source_func: impl FnMut(P) -> (D, P) + 'static,
    ) -> JetStream<'scp, D> {
        let scope = self.scope;
        let worker_index = scope.index();
        let new_stream = scope.scoped::<u64, _, _>("PersistentStateScope", move |persistent_scope| {
            // state loopback which feeds our state back to the
            // state provider, after the user modified it
            let (state_loop_handle, state_loop_cycle) = persistent_scope.feedback(1);
            let stream = self.stream
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
                                    state_backend.load_state_latest(worker_index).unwrap_or_else(P::default);
                                output.session(&new_time).give(initial_state);
                            }

                            // state loopback, this will be called everytime the state
                            // gets updated
                            while let Some((time, state)) = loopback_input.next() {
                                let new_time = time.delayed(&(time.time() + 1));
                                state.swap(&mut state_vec);
                                for s in state_vec.iter() {
                                    state_backend.save_state(worker_index, s);
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
        });
        JetStream { scope: scope, stream: new_stream }
    }
}


impl <'scp, P: PersistentState, T: Timestamp, D1: Data, D2: Data> PersistentInput<'scp, P, T, D1, D2> for JetStream<'scp, D1>
{
    fn persistent_input(
        self: Self,
        state_backend: impl PersistentStateBackend<
                T,
                P
            > + 'static,
        mut source_func: impl FnMut(P) -> (D2, P) + 'static,
    ) -> JetStream<'scp, DataUnion<D1, D2>> {
        let right_stream = JetStream::new(self.scope).persistent_source(state_backend, source_func);
        todo!()
        // self.union(right_stream)
    }
}
