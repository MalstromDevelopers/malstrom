use std::{iter, sync::{Arc, RwLock}, rc::Rc, marker::PhantomData};

use crossbeam::channel::{unbounded, Receiver, Sender};
use rand;
use rand::seq::SliceRandom;

use crate::frontier::{FrontierHandle, Frontier, Probe};
use crate::watch;

/// Data which may move through a stream
pub trait Data: Clone + 'static {}
impl<T: Clone + 'static> Data for T {}

/// Nothing
/// Important: Nothing is used as a marker type and does
/// not implement 'Data'
#[derive(Clone)]
pub struct Nothing;

/// Finalized
/// Important: Finalized is used as a marker type and does
/// not implement 'Data'
#[derive(Clone)]
pub struct Finalized;

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
    fn step(&mut self, frontiers: &mut FrontierHandle);

    /// still not happy with this function name
    fn has_queued_work(&self) -> bool;
}

/// This attempts to be the most generic operator possible
/// It has N inputs and M outputs and
/// a mapper function to map inputs to outputs
pub struct StandardOperator<I, O> {
    inputs: Vec<Receiver<I>>,
    mapper: Box<dyn FnMut(&Vec<Receiver<I>>, &Vec<Sender<O>>, &mut FrontierHandle)>,
    outputs: Vec<Sender<O>>,
}

impl< I, O, F> From<F> for StandardOperator< I, O>
where
    F: FnMut(&Vec<Receiver<I>>, &Vec<Sender<O>>) + 'static,
{
    fn from(mut mapper: F) -> StandardOperator< I, O> {
        Self::new(move |i, o, _| mapper(i, o))
    }
}

impl< I, O> StandardOperator< I, O> {
    pub fn new(mapper: impl FnMut(&Vec<Receiver<I>>, &Vec<Sender<O>>, &mut FrontierHandle) + 'static) -> Self {
        Self {
            inputs: Vec::new(),
            mapper: Box::new(mapper),
            outputs: Vec::new(),
        }
    }
}

pub trait AddOutput<O> {
    fn add_output(&mut self, tx: Sender<O>) -> ();
}

impl <I, O> AddOutput<O> for StandardOperator<I, O> {
    fn add_output(&mut self, tx: Sender<O>) {
        self.outputs.push(tx);
    }
}
pub trait AddInput<I> {
    fn add_input(&mut self, rx: Receiver<I>) -> ();
}

impl <I, O> AddInput<I> for StandardOperator<I, O> {
    fn add_input(&mut self, rx: Receiver<I>) {
        self.inputs.push(rx);
    }
}

impl< I, O> Operator for StandardOperator< I, O> {
    fn step(&mut self, frontier_handle: &mut FrontierHandle) {
        // TODO maybe the whole thing would be more efficient
        // if we used iterator instead of vec, on the other hand this way the
        // op can now where an input is from, which may be useful to track e.g.
        // for `zip` or similar
        (self.mapper)(&self.inputs, &self.outputs, frontier_handle);
    }

    fn has_queued_work(&self) -> bool {
        self.inputs.iter().any(|x| !x.is_empty())
    }
}


trait ChainableOperator<Output>: Operator + AddOutput<Output> {}
impl <Output, T: Operator + AddOutput<Output>> ChainableOperator<Output> for T {}

pub struct FrontieredOperator<Output> {
    frontier: Frontier,
    operator: Box<dyn ChainableOperator<Output>>
}

pub trait RuntimeFrontieredOperator {

    fn get_probe(&self) -> Probe;

    fn step(&mut self) -> ();

    fn has_queued_work(&self) -> bool;

    fn try_fulfill(&mut self, other_frontiers: &Vec<u64>) -> ();
}

impl<Output>  RuntimeFrontieredOperator for FrontieredOperator<Output>  {
    fn get_probe(&self) -> Probe {
        self.frontier.get_probe()
    }

    fn step(&mut self) -> () {
        let mut handle = FrontierHandle::new(&mut self.frontier);
        self.operator.step(&mut handle)
    }

    fn has_queued_work(&self) -> bool {
        self.operator.has_queued_work()
    }

    fn try_fulfill(&mut self, other_frontiers: &Vec<u64>) -> () {
        self.frontier.try_fulfill(other_frontiers)
    }
}

impl<Output> FrontieredOperator<Output> {
    fn new(operator: impl ChainableOperator<Output> + 'static) -> Self {
        Self { frontier: Frontier::default(), operator: Box::new(operator) }
    }

    pub fn add_output(&mut self, tx: Sender<Output>) {
        self.operator.add_output(tx);
    }
}

pub struct JetStream<Output> {
    operators: Vec<Box<dyn RuntimeFrontieredOperator>>,
    // these are probes for every operator in operators
    probes: Vec<Probe>,
    tail: Option<FrontieredOperator<Output>>,
    frontier: Frontier,
}

impl JetStream<Nothing> {
    pub fn new() -> Self {
        Self {
            operators: Vec::new(),
            probes: Vec::new(),
            tail: None,
            frontier: Frontier::default()
        }
    }

    pub fn from_operator<I: 'static, O: 'static>(operator: StandardOperator<I, O>) -> JetStream<O> {
        let f_operator = FrontieredOperator::new(operator);
        let probes = vec![f_operator.get_probe()];
        let tail = Some(f_operator);
        JetStream {
            operators: Vec::new(),
            probes,
            tail,
            frontier: Frontier::default()
        }
    }

    /// Add a datasource to a stream which has no data in it
    pub fn source<O: Data>(
        self,
        mut source_func: impl FnMut(&mut FrontierHandle) -> Option<O> + 'static,
    ) -> JetStream<O> {
        let operator = StandardOperator::<Nothing, O>::new(move |_inputs, outputs, frontier_handle| {
            if let Some(x) = source_func(frontier_handle) {
                dist_rand(iter::once(x), outputs)
            }
        });
        let f_operator = FrontieredOperator::<O>::new(operator);
        let mut probes = self.probes;
        probes.push(f_operator.get_probe());

        JetStream { operators: self.operators, probes, tail: Some(f_operator), frontier: self.frontier}
    }
}

impl<O> JetStream<O>
where
    O: Data,
{
    // pub fn tail(&self) -> &FrontieredOperator<O> {
    //     // we can unwrap here, since this impl block is bound by
    //     // O: Data, which means the stream already has at least
    //     // one operator
    //     &self.tail.unwrap()
    // }

    // pub fn tail_mut(&mut self) -> &mut FrontieredOperator<O> {
    //     // we can unwrap here, since this impl block is bound by
    //     // O: Data, which means the stream already has at least
    //     // one operator
    //     &mut self.tail.unwrap()
    // }

    pub fn add_output(&mut self, tx: Sender<O>) -> () {
        self.tail.as_mut().unwrap().add_output(tx)
    }

    /// add an operator to the end of this stream
    /// and return a new stream where the new operator is last_op
    pub fn then<O2: 'static>(mut self, mut operator: StandardOperator<O, O2>) -> JetStream<O2> {
        let (tx, rx) = unbounded::<O>();
        self.add_output(tx);
        let old_tail = self.tail.unwrap();

        operator.add_input(rx);
        self.operators.push(Box::new(old_tail));

        let f_operator = FrontieredOperator::<O2>::new(operator);
        self.probes.push(f_operator.get_probe());

        JetStream {
            operators: self.operators,
            probes: self.probes,
            tail: Some(f_operator),
            frontier: self.frontier
        }
    }

    pub fn finalize(mut self) -> JetStream<Finalized>{
        let tail = self.tail.unwrap();
        self.operators.push(Box::new(tail));
        JetStream { operators: self.operators, probes: self.probes, tail: None, frontier: self.frontier }
    }

}

impl JetStream<Finalized>
{
    pub fn has_queued_work(&mut self) -> bool {
        self.operators.last().unwrap().has_queued_work()
    }

    /// frontiers here are the stream level frontiers, i.e. those
    /// coming from the worker
    pub fn step(&mut self, upstream_frontiers: &Vec<u64>) {
        // it is important to step operators at least once
        // even if they don't have input, as they may be sources
        // which need to run without input
        println!("upstream_frontiers {upstream_frontiers:?}");
        for (i, op) in self.operators.iter_mut().enumerate().rev() {
            op.step();
            while op.has_queued_work() {
                op.step();
            }
            let mut upstream_worker_frontiers: Vec<u64> = self.probes[..i].iter().map(|x| x.read()).collect();
            upstream_worker_frontiers.extend(upstream_frontiers);
            op.try_fulfill(&upstream_worker_frontiers);

            let f = op.get_probe().read();
            println!("OP frontier {f}");
        }
        self.frontier.set_desired(self.probes.last().unwrap().read());
        let desired = self.probes.last().unwrap().read();
        println!("Setting desired to {desired}");
        self.frontier.try_fulfill(upstream_frontiers);
    }

    pub fn get_probe(&self) -> Probe {
        self.probes.last().unwrap().clone()
    }
}

#[cfg(test)]
mod tests {
    use crate::worker::Worker;

    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;
    use pretty_assertions::assert_eq;


    #[test]
    fn test_its_way_too_late() {
        let mut worker = Worker::new();
        let source_a = |_: &mut FrontierHandle| Some(rand::random::<f64>());
        let source_b = |_: &mut FrontierHandle| Some(rand::random::<f64>());

        let stream_a = JetStream::new().source(source_a);
        let stream_b = JetStream::new().source(source_b);
        let union_stream = worker.union(stream_a, stream_b);
    }

    #[test]
    fn test_probe() {
        let mut worker = Worker::new();

        let source = |frontier_handle: &mut FrontierHandle| {
            frontier_handle.advance_by(1).unwrap();
            Some(rand::random::<f64>())
        
        };
        let stream = JetStream::new().source(source).finalize();
        let probe = stream.get_probe();
        worker.add_stream(stream);
        assert_eq!(probe.read(), 0);
        worker.step();
        assert_eq!(probe.read(), 1);

        assert_eq!(worker.get_frontier().unwrap(), 1);
    }
}
