use std::iter;
use std::process::Output;

use crossbeam::channel::{bounded, Receiver, Sender, unbounded};
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

/// A mapper which can be applied to transform data of type A
/// into data of type B
pub trait Mapper<I, O>: 'static {
    fn apply(&mut self, inputs: &Vec<Receiver<I>>, outputs: &Vec<Sender<O>>) -> ();
}
pub struct MapperContainer<F> {
    func: F
}
impl<F> MapperContainer<F> {
    fn new(func: F) -> Self {
        Self { func }
    }
}
impl<F, I, O> Mapper<I, O> for  MapperContainer<F> where F: FnMut(&Vec<Receiver<I>>, &Vec<Sender<O>>) -> () + 'static {
    fn apply(&mut self, inputs: &Vec<Receiver<I>>, outputs: &Vec<Sender<O>>) -> () {
        (self.func)(inputs, outputs)
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

/// a helper function to distribute output randomly
pub fn dist_rand<O>(data: Vec<O>, outputs: &Vec<Sender<O>>) -> () {
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

/// The NoopMapper is a mapper which does nothing!
/// It transforms data of type A into data of type A
/// and sends it randomly to its outputs
pub struct NoopMapper;
impl NoopMapper {
    pub fn new() -> Self {
        NoopMapper {}
    }
}
impl<I> Mapper<I, I> for NoopMapper {
    fn apply(&mut self, inputs: &Vec<Receiver<I>>, outputs: &Vec<Sender<I>>) -> () {
        let data = inputs.iter().map(|x| x.try_recv().ok()).filter_map(|x| x).collect();
        dist_rand(data, outputs)
}
}

/// The source mapper will ignore any of its input data and
/// instead create any number of records out of thin air
/// and distribute them evenly across its outputs
pub struct SourceMapper<S: 'static> {
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
impl<I, O, S> Mapper<I, O> for SourceMapper<S>
where
    S: FnMut() -> Option<O>,
{
    fn apply(&mut self, _inputs: &Vec<Receiver<I>>, outputs: &Vec<Sender<O>>) -> () {
        let data = match (self.source_func)() {
            Some(x) => vec![x],
            None => vec![],
        };
        dist_rand(data, outputs)
    }
}

/// The simplest trait in the world
/// Any jetstream operator, MUST implement
/// this trait
pub trait Operator{
    /// Calling step instructs the operator, that it should attempt to make
    /// progress. There is absolutely no assumption on what "progress" means,
    /// but it is implied, that the operator reads its inputs and writes
    /// to its outputs
    fn step(&mut self) -> ();

    fn has_input(&mut self) -> bool;
}

/// This attempts to be the most generic operator possible
/// It has N inputs and M outputs and
/// a mapper function to map inputs to outputs
pub struct StandardOperator<I, O, M>
// I: Input, O: Output, M: Mapper
where
    M: Mapper<I, O>,
{
    inputs: Vec<Receiver<I>>,
    mapper: M,
    outputs: Vec<Sender<O>>,
}

impl<I, O, F> From<F> for StandardOperator<I, O, MapperContainer<F>>
where
F: FnMut(&Vec<Receiver<I>>, &Vec<Sender<O>>) -> () + 'static
{
    fn from(mapper: F) -> StandardOperator<I, O, MapperContainer<F>> {
        Self {
            inputs: Vec::new(),
            mapper: MapperContainer::new(mapper),
            outputs: Vec::new(),
        }
    }
}

impl<I, O, M> StandardOperator<I, O, M>
where
    M: Mapper<I, O>,
{

    pub fn new(mapper: M) -> Self {
        Self { inputs: Vec::new(), mapper, outputs: Vec::new() }
    }
    

    fn add_output<T, N: Mapper<O, T>>(&mut self, other: &mut StandardOperator<O, T, N>) -> () {
        let (tx, rx) = unbounded::<O>();
        self.outputs.push(tx);

        // TODO: could be done cleaner with a trait
        other.inputs.push(rx)
    }
}

impl<I, O, M> Operator for StandardOperator<I, O, M>
where
    M: Mapper<I, O>,
{
    fn step(&mut self) -> () {
        // TODO maybe the whole thing would be more efficient
        // if we used iterator instead of vec, on the other hand this way the
        // op can now where an input is from, which may be useful to track e.g.
        // for `zip` or similar
        self.mapper.apply(&self.inputs, &self.outputs);
    }

    fn has_input(&mut self) -> bool {
        self.inputs.iter().any(|x| !x.is_empty())
    }
}

/// nothing data
#[derive(Clone)]
pub struct Nothing;

/// Creates a do nothing operator.
fn noop<T>() -> StandardOperator<T, T, NoopMapper> {
    StandardOperator::new(NoopMapper::new())
}

pub struct JetStream<Input, Output, M>
where
    M: Mapper<Input, Output>,
{
    operators: Vec<Box<dyn Operator>>,
    last_op: StandardOperator<Input, Output, M>,
}

impl<T> JetStream<T, T, NoopMapper> {
    pub fn new() -> Self {
        Self {
            operators: Vec::new(),
            last_op: noop::<T>(),
        }
    }
}

impl<I, O, M> JetStream<I, O, M>
where
    I: Data,
    O: Data,
    M: Mapper<I, O>
{
    pub fn from_operator(operator: StandardOperator<I, O, M>) -> Self {
        Self {
            operators: Vec::new(),
            last_op: operator,
        }
    }

    /// Add a datasource to a stream which has no data in it
    pub fn source<S: FnMut() -> Option<O>>(
        self,
        source_func: S,
    ) -> JetStream<Nothing, O, SourceMapper<S>> {
        let last_op = StandardOperator::new(SourceMapper::new(source_func));

        let mut operators = self.operators;
        operators.push(Box::new(self.last_op));
        JetStream { operators, last_op }
    }

    /// add an operator to the end of this stream
    /// and return a new stream where the new operator is last_op
    pub fn then<O2, M2: Mapper<O, O2>>(
        self,
        mut operator: StandardOperator<O, O2, M2>,
    ) -> JetStream<O, O2, M2> {
        let mut last_op = self.last_op;
        let mut operators = self.operators;
        last_op.add_output(&mut operator);
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
    fn side<P: Data, M2: Mapper<O, P>>(
        &mut self,
        mut operator: StandardOperator<O, P, M2>,
    ) -> () {
        self.last_op.add_output(&mut operator);
        self.operators.push(Box::new(operator))
    }

    pub fn step(&mut self) -> () {

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

impl<Input, Output, M> JetStream<Input, Output, M>
where
    Input: Data,
    Output: Data,
    M: Mapper<Input, Output>,
{
    /// union two streams
    pub fn union<
        InputB: Data,
        OutputB: Data,
        MB: Mapper<InputB, OutputB>
    >(
        &mut self,
        other: &mut JetStream<InputB, OutputB, MB>,
    ) -> JetStream<
        DataUnion<Output, OutputB>,
        DataUnion<Output, OutputB>,
        NoopMapper
    > {
        let mut new_stream = JetStream::new();

        // wrap data from left into a data union
        let mut transform_left = StandardOperator::from(
            |inputs: &Vec<Receiver<Output>>, outputs: &Vec<Sender<DataUnion<Output, OutputB>>>| {
                    let data = inputs.iter().map(|x| x.try_recv().ok())
                    .filter_map(|x| x.and_then(|y| Some(DataUnion::Left(y))))
                    .collect();
                    dist_rand(data, outputs)
            },
        );
        // wrap data from right into a data union
        let mut transform_right = StandardOperator::from(
            |inputs: &Vec<Receiver<OutputB>>, outputs: &Vec<Sender<DataUnion<Output, OutputB>>>| {
                    let data = inputs.iter().map(|x| x.try_recv().ok())
                    .filter_map(|x| x.and_then(|y| Some(DataUnion::Right(y))))
                    .collect();
                    dist_rand(data, outputs)
            },
        );

        transform_left.add_output(&mut new_stream.last_op);
        transform_right.add_output(&mut new_stream.last_op);

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
