use crate::channels::selective_broadcast::{full_broadcast, Receiver, Sender};
/// Operators:
/// Lifecycle:
/// OperatorBuilder -> Appendable Operator -> FrontieredOperator
/// transforms it into the immutable FrontieredOperator
/// which can be used at runtime
use crate::frontier::{Frontier, FrontierHandle, Probe, Timestamp};
use crate::snapshot::backend::{NoState, PersistenceBackend, State};
use crate::snapshot::barrier::BarrierData;

use super::jetstream::Data;

/// An Operator which can have output added and can be turned
/// into a FrontieredOperator
/// This trait exists mainly for type erasure, so that the Jetstream
/// need not know the input type of its last operator
pub trait AppendableOperator<O, P: PersistenceBackend> {
    fn get_output_mut(&mut self) -> &mut Sender<O>;

    fn build(self: Box<Self>) -> FrontieredOperator<P>;
}

/// An Operator which does nothing except passing data along
pub fn pass_through_operator<T: Data>() -> StandardOperator<T, T, NoState> {
    StandardOperator::new(|input, frontier_handle, _| {
        frontier_handle.advance_to(Timestamp::MAX);
        input
    })
}

/// A builder type to build generic operators
pub struct StandardOperator<I, O, S> {
    input: Receiver<I>,
    mapper: Box<dyn FnMut(Option<I>, &mut FrontierHandle, &mut S) -> Option<O>>,
    output: Sender<O>,
    // state the mapper has access to
    state: Option<S>,
}

impl<I, O, S> StandardOperator<I, O, S>
where
    I: Data,
    O: Data,
    S: State,
{
    pub fn new(
        mapper: impl FnMut(Option<I>, &mut FrontierHandle, &mut S) -> Option<O> + 'static,
    ) -> Self {
        let input = Receiver::new_unlinked();
        let output = Sender::new_unlinked(full_broadcast);
        Self {
            input,
            mapper: Box::new(mapper),
            output,
            state: None,
        }
    }

    pub fn new_with_partitioning(
        mapper: impl FnMut(Option<I>, &mut FrontierHandle, &mut S) -> Option<O> + 'static,
        partitioner: impl Fn(&O, usize) -> Vec<usize> + 'static,
    ) -> Self {
        let input = Receiver::new_unlinked();
        let output = Sender::new_unlinked(partitioner);
        Self {
            input,
            mapper: Box::new(mapper),
            output,
            state: None,
        }
    }

    pub fn get_input_mut(&mut self) -> &mut Receiver<I> {
        &mut self.input
    }
}

impl<I, O, S, P> AppendableOperator<O, P> for StandardOperator<I, O, S>
where
    I: 'static,
    O: Data + 'static,
    S: State,
    P: PersistenceBackend,
{
    fn get_output_mut(&mut self) -> &mut Sender<O> {
        &mut self.output
    }

    fn build(self: Box<Self>) -> FrontieredOperator<P> {
        // let operator = StandardOperator{input: self.input, mapper: self.mapper, output: self.output};
        FrontieredOperator::new(*self)
    }
}

/// Type only used internally
// struct StandardOperator<I, O> {
//     input: Receiver<I>,
//     mapper: Box<dyn FnMut(&mut Receiver<I>, &mut Sender<O>, &mut FrontierHandle)>,
//     output: Sender<O>,
// }
impl<I, O, S, P> Operator<P> for StandardOperator<I, O, S>
where
    O: Clone,
    P: PersistenceBackend,
    S: State,
{
    fn step(
        &mut self,
        frontier_handle: &mut FrontierHandle,
        persistence_key: usize,
        persistence_backend: &mut P,
    ) {
        let mut state = self.state.get_or_insert_with(|| {
            persistence_backend
                .load::<S>(persistence_key)
                .unwrap_or_default()
        });
        match self.input.recv() {
            Some(BarrierData::Barrier(b)) => {
                persistence_backend.persist(
                    persistence_key,
                    frontier_handle.get_actual(),
                    &state,
                    b,
                );
                self.output.send(BarrierData::Barrier(b))
            }
            Some(BarrierData::Data(d)) => {
                if let Some(x) = (self.mapper)(Some(d), frontier_handle, &mut state) {
                    self.output.send(BarrierData::Data(x))
                }
            }
            None => {
                if let Some(x) = (self.mapper)(None, frontier_handle, &mut state) {
                    self.output.send(BarrierData::Data(x))
                }
            }
        }
    }

    fn has_queued_work(&self) -> bool {
        !self.input.is_empty()
    }
}

pub struct FrontieredOperator<P: PersistenceBackend> {
    frontier: Frontier,
    operator: Box<dyn Operator<P>>,
}

impl<P> FrontieredOperator<P>
where
    P: PersistenceBackend,
{
    fn new<I: 'static, O: Data + 'static, S: State>(operator: StandardOperator<I, O, S>) -> Self {
        Self {
            frontier: Frontier::default(),
            operator: Box::new(operator),
        }
    }

    pub fn add_upstream_probe(&mut self, probe: Probe) -> () {
        self.frontier.add_upstream_probe(probe)
    }
}
impl<P> RuntimeFrontieredOperator<P> for FrontieredOperator<P>
where
    P: PersistenceBackend,
{
    fn get_probe(&self) -> Probe {
        self.frontier.get_probe()
    }

    fn step(&mut self, persistence_key: usize, persistence_backend: &mut P) {
        let mut handle = FrontierHandle::new(&mut self.frontier);
        self.operator
            .step(&mut handle, persistence_key, persistence_backend)
    }

    fn has_queued_work(&self) -> bool {
        self.operator.has_queued_work()
    }
}

/// The simplest trait in the world
/// Any jetstream operator, MUST implement
/// this trait
pub trait Operator<P: PersistenceBackend> {
    /// Calling step instructs the operator, that it should attempt to make
    /// progress. There is absolutely no assumption on what "progress" means,
    /// but it is implied, that the operator reads its input and writes
    /// to its output
    fn step(
        &mut self,
        frontiers: &mut FrontierHandle,
        persistence_key: usize,
        persistence_backend: &mut P,
    );

    /// still not happy with this function name
    fn has_queued_work(&self) -> bool;
}
pub trait RuntimeFrontieredOperator<P>
where
    P: PersistenceBackend,
{
    fn get_probe(&self) -> Probe;

    fn step(&mut self, persistence_key: usize, persistence_backend: &mut P);

    fn has_queued_work(&self) -> bool;
}
