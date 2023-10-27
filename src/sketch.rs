use std::process::Output;

use crossbeam::channel::{bounded, Receiver, Sender};
use rand;
use rand::seq::SliceRandom;

/// Data which may move through a stream
pub trait Data: Clone + 'static {}
impl<T: Clone + 'static> Data for T {}

/// A union of two data types
/// useful when combining streams
#[derive(Clone, Copy)]
enum DataUnion<L, R> {
    Left(L),
    Right(R),
}

/// A mapper which can be applied to transform data of type A
/// into data of type B
trait Mapper<I, O>: 'static {
    fn apply(&mut self, _: Vec<Option<I>>) -> Vec<O>;
}
impl<I, O, S> Mapper<I, O> for S
where
    S: FnMut(Vec<Option<I>>) -> Vec<O> + 'static,
{
    fn apply(&mut self, values: Vec<Option<I>>) -> Vec<O> {
        self(values)
    }
}

/// A distributor, which is some strategy to distribute N records
/// to M outputs
trait Distributor<O>: 'static {
    fn distribute(&self, values: Vec<O>, outputs: &Vec<Sender<O>>);
}

/// The RandomDistributor is a distributor which will distribute
/// all records randomly across its outputs without duplicating
/// or creating any records
struct RandomDistributor;
impl RandomDistributor {
    pub fn new() -> Self {
        RandomDistributor {}
    }
}
impl<T> Distributor<T> for RandomDistributor {
    fn distribute(&self, data: Vec<T>, outputs: &Vec<Sender<T>>) -> () {
        if outputs.len() == 1 {
            for d in data.into_iter() {
                outputs.first().unwrap().send(d);
            }
            return;
        }

        for d in data.into_iter() {
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
}

/// The NoopMapper is a mapper which does nothing!
/// It transforms data of type A into data of type A
struct NoopMapper;
impl NoopMapper {
    pub fn new() -> Self {
        NoopMapper {}
    }
}
impl<I> Mapper<I, I> for NoopMapper {
    fn apply(&mut self, data: Vec<Option<I>>) -> Vec<I> {
        data.into_iter().filter_map(|x| x).collect()
    }
}

/// The source mapper will ignore any of its input data and
/// instead create any number of records out of thin air
struct SourceMapper<S: 'static> {
    source_func: S,
}
impl<S, T> SourceMapper<S>
where
    S: FnMut() -> Option<T>,
{
    pub fn new(source_func: S) -> Self {
        SourceMapper { source_func }
    }
}
impl<I, T, S> Mapper<I, T> for SourceMapper<S>
where
    S: FnMut() -> Option<T>,
{
    fn apply(&mut self, _: Vec<Option<I>>) -> Vec<T> {
        match (self.source_func)() {
            Some(x) => vec![x],
            None => vec![],
        }
    }
}

/// The simplest trait in the world
/// Any jetstream operator, MUST implement
/// this trait
pub trait Operate {
    /// Calling step instructs the operator, that it should attempt to make
    /// progress. There is absolutely no assumption on what "progress" means,
    /// but it is implied, that the operator reads its inputs and writes
    /// to its outputs
    fn step(&mut self) -> ();
}

/// This attempts to be the most generic operator possible
/// It has N inputs and M outputs
/// A mapper function to mapper data
/// A distributor to distribute the transformed data to the outputs
struct Operator<I, O, M, D>
// I: Input, O: Output, F: op, D: Distributor
where
    M: Mapper<I, O>,
    D: Distributor<O>,
{
    inputs: Vec<Receiver<I>>,
    op: M,
    distributor: D,
    outputs: Vec<Sender<O>>,
}

impl<I, O, M, D> Operator<I, O, M, D>
where
    M: Mapper<I, O>,
    D: Distributor<O>,
{
    pub fn new(op: M, distributor: D) -> Self {
        Self {
            inputs: Vec::new(),
            op: op,
            distributor: distributor,
            outputs: Vec::new(),
        }
    }

    pub fn add_downstream<T, U: Mapper<O, T>, V: Distributor<T>>(
        &mut self,
        other: &mut Operator<O, T, U, V>,
    ) {
        let (tx, rx) = bounded::<O>(1);
        // and now kiss!
        self.outputs.push(tx);
        other.inputs.push(rx)
    }
}

impl<I, O, M, D> Operate for Operator<I, O, M, D>
where
    M: Mapper<I, O>,
    D: Distributor<O>,
{
    fn step(&mut self) -> () {
        // TODO maybe the whole thing would be more efficient
        // if we used iterator instead of vec, on the other hand this way the
        // op can now where an input is from, which may be useful to track e.g.
        // for `zip` or similar
        let in_data: Vec<Option<I>> = self.inputs.iter().map(|rx| rx.try_recv().ok()).collect();
        let transformed = self.op.apply(in_data);
        self.distributor.distribute(transformed, &self.outputs)
    }
}

/// nothing data
#[derive(Clone)]
struct Nothing;

/// Creates a do nothing operator.
fn noop<T>() -> Operator<T, T, NoopMapper, RandomDistributor> {
    Operator::new(NoopMapper::new(), RandomDistributor::new())
}

struct JetStream<Input, Output, M, D>
where
    M: Mapper<Input, Output>,
    D: Distributor<Output>,
{
    operators: Vec<Box<dyn Operate>>,
    last_op: Operator<Input, Output, M, D>,
}

impl<T> JetStream<T, T, NoopMapper, RandomDistributor> {
    pub fn new() -> Self {
        Self {
            operators: Vec::new(),
            last_op: noop::<T>(),
        }
    }
}

impl<I, O, M, D> JetStream<I, O, M, D>
where
    I: Data,
    O: Data,
    M: Mapper<I, O>,
    D: Distributor<O>,
{
    pub fn from_operator(operator: Operator<I, O, M, D>) -> Self {
        Self {
            operators: Vec::new(),
            last_op: operator,
        }
    }

    /// Add a datasource to a stream which has no data in it
    pub fn source<S: Fn() -> Option<O>>(
        self,
        source_func: S,
    ) -> JetStream<Nothing, O, SourceMapper<S>, RandomDistributor> {
        let last_op = Operator::new(SourceMapper::new(source_func), RandomDistributor::new());

        let mut operators = self.operators;
        operators.push(Box::new(self.last_op));
        JetStream { operators, last_op }
    }

    /// add an operator to the end of this stream
    /// and return a new stream where the new operator is last_op
    fn then<P, M2: Mapper<O, P>, D2: Distributor<P>>(
        self,
        mut operator: Operator<O, P, M2, D2>,
    ) -> JetStream<O, P, M2, D2> {
        let mut last_op = self.last_op;
        let mut operators = self.operators;
        last_op.add_downstream(&mut operator);
        operators.push(Box::new(last_op));
        JetStream {
            operators,
            last_op: operator,
        }
    }

    /// add an operator to the outputs of our current last_op
    /// and add it to our Vec of operators, but don't change
    /// the last op
    /// This is useful for side outputs
    fn side<P: Data, M2: Mapper<O, P>, D2: Distributor<P>>(
        &mut self,
        mut operator: Operator<O, P, M2, D2>,
    ) -> () {
        self.last_op.add_downstream(&mut operator);
        self.operators.push(Box::new(operator))
    }
}

impl<Input, Output, M, D> JetStream<Input, Output, M, D>
where
    Input: Data,
    Output: Data,
    M: Mapper<Input, Output>,
    D: Distributor<Output>,
{
    /// union two streams
    pub fn union<
        InputB: Data,
        OutputB: Data,
        MB: Mapper<InputB, OutputB>,
        DB: Distributor<OutputB>,
    >(
        &mut self,
        other: &mut JetStream<InputB, OutputB, MB, DB>,
    ) -> JetStream<
        DataUnion<Output, OutputB>,
        DataUnion<Output, OutputB>,
        NoopMapper,
        RandomDistributor,
    > {
        let mut new_stream = JetStream::new();
        let mut transform_left = Operator::new(
            |v: Vec<Option<Output>>| {
                v.into_iter()
                    .filter_map(|x| x.and_then(|y| Some(DataUnion::Left(y))))
                    .collect()
            },
            RandomDistributor::new(),
        );
        let mut transform_right = Operator::new(
            |v: Vec<Option<OutputB>>| {
                v.into_iter()
                    .filter_map(|x| x.and_then(|y| Some(DataUnion::Right(y))))
                    .collect()
            },
            RandomDistributor::new(),
        );

        transform_left.add_downstream(&mut new_stream.last_op);
        transform_right.add_downstream(&mut new_stream.last_op);

        self.side(transform_left);
        other.side(transform_right);
        new_stream
    }
}

#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;

    #[test]
    fn test_its_way_too_late() {
        let source_a = || Some(rand::random::<f64>());
        let source_b = || Some(rand::random::<f64>());

        let mut stream_a = JetStream::new().source(source_a);
        let mut stream_b = JetStream::new().source(source_b);
        let union_stream = stream_a.union(&mut stream_b);
    }
}
