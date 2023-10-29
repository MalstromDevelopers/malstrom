use std::iter;

use crossbeam::channel::{unbounded, Receiver, Sender};
use rand;
use rand::seq::SliceRandom;

/// Data which may move through a stream
pub trait Data: Clone + 'static {}
impl<T: Clone + 'static> Data for T {}

/// A union of two data types
/// useful when combining streams
#[derive(Clone, Copy)]
pub enum DataUnion<L, R> {
    Left(L),
    Right(R),
}

/// a helper function to distribute output randomly
pub fn dist_rand<O>(data: impl Iterator<Item = O>, outputs: &Vec<Sender<O>>) {
    for d in data {
        if let Some(tx) = outputs.choose(&mut rand::thread_rng()) {
            // TODO: currently we just crash if a downstream op is unavailable
            // but we may want to try sending to a different one instead
            tx.send(d).expect("Failed to send data downstream");
        } else {
            // no outputs
            return;
        }
    }
}
/// The simplest trait in the world
/// Any jetstream operator, MUST implement
/// this trait
pub trait Operator {
    /// Calling step instructs the operator, that it should attempt to make
    /// progress. There is absolutely no assumption on what "progress" means,
    /// but it is implied, that the operator reads its inputs and writes
    /// to its outputs
    fn step(&mut self);

    fn has_input(&mut self) -> bool;
}

/// This attempts to be the most generic operator possible
/// It has N inputs and M outputs and
/// a mapper function to map inputs to outputs
pub struct StandardOperator<I, O> {
    inputs: Vec<Receiver<I>>,
    mapper: Box<dyn FnMut(&Vec<Receiver<I>>, &Vec<Sender<O>>)>,
    outputs: Vec<Sender<O>>,
}

impl<I, O, F> From<F> for StandardOperator<I, O>
where
    F: FnMut(&Vec<Receiver<I>>, &Vec<Sender<O>>) + 'static,
{
    fn from(mapper: F) -> StandardOperator<I, O> {
        Self::new(mapper)
    }
}

impl<I, O> StandardOperator<I, O> {
    pub fn new(mapper: impl FnMut(&Vec<Receiver<I>>, &Vec<Sender<O>>) + 'static) -> Self {
        Self {
            inputs: Vec::new(),
            mapper: Box::new(mapper),
            outputs: Vec::new(),
        }
    }

    pub fn add_output<T>(&mut self, other: &mut StandardOperator<O, T>) {
        let (tx, rx) = unbounded::<O>();
        self.outputs.push(tx);

        // TODO: could be done cleaner with a trait
        other.inputs.push(rx)
    }
}

impl<I, O> Operator for StandardOperator<I, O> {
    fn step(&mut self) {
        // TODO maybe the whole thing would be more efficient
        // if we used iterator instead of vec, on the other hand this way the
        // op can now where an input is from, which may be useful to track e.g.
        // for `zip` or similar
        (self.mapper)(&self.inputs, &self.outputs);
    }

    fn has_input(&mut self) -> bool {
        self.inputs.iter().any(|x| !x.is_empty())
    }
}

/// nothing data
#[derive(Clone)]
pub struct Nothing;

/// Creates a do nothing operator.
fn noop<T>() -> StandardOperator<T, T> {
    StandardOperator::new(|_inputs, _outputs| {})
}

pub struct JetStream<Input, Output> {
    operators: Vec<Box<dyn Operator>>,
    last_op: StandardOperator<Input, Output>,
}

impl<T> JetStream<T, T> {
    pub fn new() -> Self {
        Self {
            operators: Vec::new(),
            last_op: noop::<T>(),
        }
    }
}

impl<I, O> JetStream<I, O>
where
    I: Data,
    O: Data,
{
    pub fn from_operator(operator: StandardOperator<I, O>) -> Self {
        Self {
            operators: Vec::new(),
            last_op: operator,
        }
    }

    pub fn tail_mut(&mut self) -> &mut StandardOperator<I, O> {
        &mut self.last_op
    }

    /// Add a datasource to a stream which has no data in it
    pub fn source(
        self,
        mut source_func: impl FnMut() -> Option<O> + 'static,
    ) -> JetStream<Nothing, O> {
        let last_op = StandardOperator::new(move |_inputs, outputs| {
            if let Some(x) = source_func() {
                dist_rand(iter::once(x), outputs)
            }
        });

        let mut operators = self.operators;
        operators.push(Box::new(self.last_op));
        JetStream { operators, last_op }
    }

    /// add an operator to the end of this stream
    /// and return a new stream where the new operator is last_op
    pub fn then<O2>(self, mut operator: StandardOperator<O, O2>) -> JetStream<O, O2> {
        let mut last_op = self.last_op;
        let mut operators = self.operators;
        last_op.add_output(&mut operator);
        operators.push(Box::new(last_op));
        JetStream {
            operators,
            last_op: operator,
        }
    }
}

impl<I, O> Operator for JetStream<I, O> {
    fn has_input(&mut self) -> bool {
        self.last_op.has_input()
    }

    fn step(&mut self) {
        // it is important to step operators at least once
        // even if they don't have input, as they may be sources
        // which need to run without input
        self.last_op.step();
        if self.last_op.has_input() {
            return;
        }
        for operator in self.operators.iter_mut().rev() {
            operator.step();
            if operator.has_input() {
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;

    #[test]
    fn test_its_way_too_late() {
        let mut worker = Worker::new();
        let source_a = || Some(rand::random::<f64>());
        let source_b = || Some(rand::random::<f64>());

        let mut stream_a = JetStream::new().source(source_a);
        let mut stream_b = JetStream::new().source(source_b);
        let union_stream = worker.union(stream_a, stream_b);
    }
}
