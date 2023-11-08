/// Operators:
/// Lifecycle:
/// OperatorBuilder -> Appendable Operator -> FrontieredOperator
/// transforms it into the immutable FrontieredOperator
/// which can be used at runtime

use crate::frontier::{FrontierHandle, Frontier, Probe};
use crate::channels::selective_broadcast::{Sender, Receiver, full_broadcast};

use super::jetstream::Data;

/// An Operator which can have output added and can be turned
/// into a FrontieredOperator
/// This trait exists mainly for type erasure, so that the Jetstream
/// need not know the input type of its last operator
pub trait AppendableOperator<O> {
    fn get_output_mut(&mut self) -> &mut Sender<O>;

    fn build(self: Box<Self>) -> FrontieredOperator;
}

/// An Operator which does nothing except passing data along
pub fn pass_through_operator<T: Data>() -> StandardOperator<T, T> {
    StandardOperator::from(
    |input: &mut Receiver<T>, output: &mut Sender<T>| {
        if let Some(x) = input.recv() {
            output.send(x)
        }
    }
)
}

/// A builder type to build generic operators
pub struct StandardOperator<I, O> {
    input: Receiver<I>,
    mapper: Box<dyn FnMut(&mut Receiver<I>, &mut Sender<O>, &mut FrontierHandle)>,
    output: Sender<O>
}

impl<I, O> StandardOperator<I, O> where I: Data, O: Data {

    pub fn new(mapper: impl FnMut(&mut Receiver<I>, &mut Sender<O>, &mut FrontierHandle) + 'static) -> Self {
        let input = Receiver::new_unlinked();
        let output = Sender::new_unlinked(full_broadcast);
        Self {input, mapper: Box::new(mapper), output}
    }

    pub fn new_with_partitioning(mapper: impl FnMut(&mut Receiver<I>, &mut Sender<O>, &mut FrontierHandle) + 'static, partitioner: impl Fn(&O, usize) -> Vec<usize> + 'static) -> Self {
        let input = Receiver::new_unlinked();
        let output = Sender::new_unlinked(partitioner);
        Self {input, mapper: Box::new(mapper), output}
    }

    pub fn get_input_mut(&mut self) -> &mut Receiver<I> {
        &mut self.input
    }
}

impl <I, O> AppendableOperator<O> for StandardOperator<I, O> where I: 'static, O: 'static{
    fn get_output_mut(&mut self) -> &mut Sender<O> {
        &mut self.output
    }

    fn build(self: Box<Self>) -> FrontieredOperator {
        // let operator = StandardOperator{input: self.input, mapper: self.mapper, output: self.output};
        FrontieredOperator::new(*self)
    }
}

impl< I, O, F> From<F> for StandardOperator< I, O>
where
    F: FnMut(&mut Receiver<I>, &mut Sender<O>) + 'static,
    I: Data,
    O: Data
{
    fn from(mut mapper: F) -> StandardOperator< I, O> {
        Self::new(move |i, o, _| mapper(i, o))
    }
}

/// Type only used internally
// struct StandardOperator<I, O> {
//     input: Receiver<I>,
//     mapper: Box<dyn FnMut(&mut Receiver<I>, &mut Sender<O>, &mut FrontierHandle)>,
//     output: Sender<O>,
// }
impl<I, O> Operator for StandardOperator< I, O> {
    fn step(&mut self, frontier_handle: &mut FrontierHandle) {
        (self.mapper)(&mut self.input, &mut self.output, frontier_handle);
    }

    fn has_queued_work(&self) -> bool {
        !self.input.is_empty()
    }
}

pub struct FrontieredOperator {
    frontier: Frontier,
    operator: Box<dyn Operator>
}

impl FrontieredOperator {
    fn new<I: 'static, O: 'static>(operator: StandardOperator<I, O>) -> Self {
        Self { frontier: Frontier::default(), operator: Box::new(operator) }
    }
}
impl RuntimeFrontieredOperator for FrontieredOperator  {
    fn get_probe(&self) -> Probe {
        self.frontier.get_probe()
    }

    fn step(&mut self) {
        let mut handle = FrontierHandle::new(&mut self.frontier);
        self.operator.step(&mut handle)
    }

    fn has_queued_work(&self) -> bool {
        self.operator.has_queued_work()
    }

    fn try_fulfill(&mut self, other_frontiers: &Vec<u64>) {
        self.frontier.try_fulfill(other_frontiers)
    }
}

/// The simplest trait in the world
/// Any jetstream operator, MUST implement
/// this trait
pub trait Operator {
    /// Calling step instructs the operator, that it should attempt to make
    /// progress. There is absolutely no assumption on what "progress" means,
    /// but it is implied, that the operator reads its input and writes
    /// to its output
    fn step(&mut self, frontiers: &mut FrontierHandle);

    /// still not happy with this function name
    fn has_queued_work(&self) -> bool;
}
pub trait RuntimeFrontieredOperator {

    fn get_probe(&self) -> Probe;

    fn step(&mut self);

    fn has_queued_work(&self) -> bool;

    fn try_fulfill(&mut self, other_frontiers: &Vec<u64>);
}
