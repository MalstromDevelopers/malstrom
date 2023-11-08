use std::{iter, sync::{Arc, RwLock}, rc::Rc, marker::PhantomData, process::Output};

use crate::channels::selective_broadcast::{Receiver, Sender, unbounded, self};
use rand;
use rand::seq::SliceRandom;

use crate::frontier::{FrontierHandle, Frontier, Probe};

use super::operator::{StandardOperator, RuntimeFrontieredOperator, FrontieredOperator, AppendableOperator};
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

pub struct JetStreamEmpty;

impl JetStreamEmpty {
    pub fn new() -> Self {
        Self {}
    }
    /// Add a datasource to a stream which has no data in it
    pub fn source<O: Data>(
        self,
        mut source_func: impl FnMut(&mut FrontierHandle) -> Option<O> + 'static,
    ) -> JetStreamBuilder<O> {
        let operator = StandardOperator::<Nothing, O>::new(move |_input, output, frontier_handle| {
            if let Some(x) = source_func(frontier_handle) {
                output.send(x)
            }
        });
        JetStreamBuilder { operators: Vec::new(), probes: Vec::new(), tail: Box::new(operator)}
    }
}

pub struct JetStreamBuilder<Output> {
    operators: Vec<FrontieredOperator>,
    // these are probes for every operator in operators
    probes: Vec<Probe>,
    tail: Box<dyn AppendableOperator<Output>>,
}

impl JetStreamBuilder<Output> {

    pub fn from_operator<I: 'static, O: 'static>(operator: StandardOperator<I, O>) -> JetStreamBuilder<O> {
        JetStreamBuilder {
            operators: Vec::new(),
            probes: Vec::new(),
            tail: Box::new(operator),
        }
    }

}

impl<O> JetStreamBuilder<O>
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

    pub fn get_output_mut(&mut self,) -> &mut Sender<O> {
        self.tail.get_output_mut()
    }

    /// add an operator to the end of this stream
    /// and return a new stream where the new operator is last_op
    pub fn then<O2: Data + 'static>(mut self, mut operator: StandardOperator<O, O2>) -> JetStreamBuilder<O2> {
        // let (tx, rx) = selective_broadcast::<O>();
        selective_broadcast::link(self.tail.get_output_mut(), operator.get_input_mut());

        let old_tail_f = self.tail.build();

        self.probes.push(old_tail_f.get_probe());
        self.operators.push(old_tail_f);

        JetStreamBuilder {
            operators: self.operators,
            probes: self.probes,
            tail: Box::new(operator)
        }
    }

    pub fn build(mut self) -> JetStream {
        let tail = self.tail.build();
        self.probes.push(tail.get_probe());
        self.operators.push(tail);
        JetStream { operators: self.operators, probes: self.probes, frontier: Frontier::default() }
    }

}

pub struct JetStream {
    operators: Vec<FrontieredOperator>,
    // these are probes for every operator in operators
    probes: Vec<Probe>,
    frontier: Frontier,
}

impl JetStream
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

        let stream_a = JetStreamEmpty::new().source(source_a);
        let stream_b = JetStreamEmpty::new().source(source_b);
        let union_stream = worker.union_n([stream_a, stream_b]);
    }

    #[test]
    fn test_probe() {
        let mut worker = Worker::new();

        let source = |frontier_handle: &mut FrontierHandle| {
            frontier_handle.advance_by(1).unwrap();
            Some(rand::random::<f64>())
        
        };
        let stream = JetStreamEmpty::new().source(source).build();
        let probe = stream.get_probe();
        worker.add_stream(stream);
        assert_eq!(probe.read(), 0);
        worker.step();
        assert_eq!(probe.read(), 1);

        assert_eq!(worker.get_frontier().unwrap(), 1);
    }
}
