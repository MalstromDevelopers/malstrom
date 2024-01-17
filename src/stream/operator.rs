use crate::channels::selective_broadcast::{full_broadcast, Receiver, Sender};
/// Operators:
/// Lifecycle:
/// OperatorBuilder -> Appendable Operator -> FrontieredOperator
/// transforms it into the immutable FrontieredOperator
/// which can be used at runtime
use crate::frontier::{Frontier, FrontierHandle, Probe, Timestamp};
use crate::snapshot::PersistenceBackend;

use super::jetstream::Data;

/// An Operator which can have output added and can be turned
/// into a FrontieredOperator
/// This trait exists mainly for type erasure, so that the Jetstream
/// need not know the input type of its last operator
pub trait AppendableOperator<O, P: PersistenceBackend> {
    fn get_output_mut(&mut self) -> &mut Sender<O, P>;

    fn build(self: Box<Self>) -> FrontieredOperator<P>;
}

/// An Operator which does nothing except passing data along
pub fn pass_through_operator<T: Data, P: PersistenceBackend>() -> StandardOperator<T, T, P> {
    StandardOperator::new(|input, output, frontier, _| {
        frontier.advance_to(Timestamp::MAX);
        if let Some(x) = input.recv() {
            output.send(x)
        }
    })
}

/// A builder type to build generic operators
pub struct StandardOperator<I, O, P: PersistenceBackend> {
    input: Receiver<I, P>,
    mapper: Box<dyn Mapper<I, O, P>>,
    output: Sender<O, P>,
}

pub trait Mapper<I, O, P>:
    FnMut(&mut Receiver<I, P>, &mut Sender<O, P>, &mut FrontierHandle, usize) + 'static
{
}
impl<
        I,
        O,
        P,
        T: FnMut(&mut Receiver<I, P>, &mut Sender<O, P>, &mut FrontierHandle, usize) + 'static,
    > Mapper<I, O, P> for T
{
}

impl<I, O, P> StandardOperator<I, O, P>
where
    I: Data,
    O: Data,
    P: PersistenceBackend,
{
    pub fn new(mapper: impl Mapper<I, O, P>) -> Self {
        let input = Receiver::new_unlinked();
        let output = Sender::new_unlinked(full_broadcast);
        Self {
            input,
            mapper: Box::new(mapper),
            output,
        }
    }

    pub fn new_with_partitioning(
        mapper: impl Mapper<I, O, P>,
        partitioner: impl Fn(&O, usize) -> Vec<usize> + 'static,
    ) -> Self {
        let input = Receiver::new_unlinked();
        let output = Sender::new_unlinked(partitioner);
        Self {
            input,
            mapper: Box::new(mapper),
            output,
        }
    }

    pub fn get_input_mut(&mut self) -> &mut Receiver<I, P> {
        &mut self.input
    }
}

impl<I, O, P> AppendableOperator<O, P> for StandardOperator<I, O, P>
where
    I: 'static,
    O: Data + 'static,
    P: PersistenceBackend,
{
    fn get_output_mut(&mut self) -> &mut Sender<O, P> {
        &mut self.output
    }

    fn build(self: Box<Self>) -> FrontieredOperator<P> {
        // let operator = StandardOperator{input: self.input, mapper: self.mapper, output: self.output};
        FrontieredOperator::new(*self)
    }
}

/// Type only used internally
// struct StandardOperator<I, O> {
//     input: Receiver<I, P>,
//     mapper: Box<dyn FnMut(&mut Receiver<I, P>, &mut Sender<O, P>, &mut FrontierHandle)>,
//     output: Sender<O, P>,
// }
impl<I, O, P> Operator<P> for StandardOperator<I, O, P>
where
    O: Clone,
    P: PersistenceBackend,
{
    fn step(&mut self, frontier_handle: &mut FrontierHandle, operator_id: usize) {
        (self.mapper)(
            &mut self.input,
            &mut self.output,
            frontier_handle,
            operator_id,
        );
    }

    fn has_queued_work(&self) -> bool {
        !self.input.is_empty()
    }
}

pub struct FrontieredOperator<P> {
    frontier: Frontier,
    operator: Box<dyn Operator<P>>,
}

impl<P> FrontieredOperator<P>
where
    P: PersistenceBackend,
{
    fn new<I: 'static, O: Data + 'static>(operator: StandardOperator<I, O, P>) -> Self {
        Self {
            frontier: Frontier::default(),
            operator: Box::new(operator),
        }
    }

    pub fn add_upstream_probe(&mut self, probe: Probe) {
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

    fn step(&mut self, operator_id: usize) {
        let mut handle = FrontierHandle::new(&mut self.frontier);
        self.operator.step(&mut handle, operator_id)
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
    fn step(&mut self, frontiers: &mut FrontierHandle, operator_id: usize);

    /// still not happy with this function name
    fn has_queued_work(&self) -> bool;
}
pub trait RuntimeFrontieredOperator<P>
where
    P: PersistenceBackend,
{
    fn get_probe(&self) -> Probe;

    fn step(&mut self, operator_id: usize);

    fn has_queued_work(&self) -> bool;
}
