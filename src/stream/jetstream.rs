use std::process::Output;

use crate::{
    channels::selective_broadcast::{self, Sender},
    frontier::Probe,
};
use rand;

use crate::frontier::FrontierHandle;

use super::operator::{AppendableOperator, FrontieredOperator, StandardOperator};
/// Data which may move through a stream
pub trait Data: Clone + 'static {}
impl<T: Clone + 'static> Data for T {}

/// Nothing
/// Important: Nothing is used as a marker type and does
/// not implement 'Data'
#[derive(Clone)]
pub struct Nothing;

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
        let operator =
            StandardOperator::<Nothing, O>::new(move |_input, output, frontier_handle| {
                if let Some(x) = source_func(frontier_handle) {
                    output.send(x)
                }
            });
        JetStreamBuilder {
            operators: Vec::new(),
            tail: Box::new(operator),
        }
    }
}

pub struct JetStreamBuilder<Output> {
    operators: Vec<FrontieredOperator>,
    // these are probes for every operator in operators
    tail: Box<dyn AppendableOperator<Output>>,
}

impl JetStreamBuilder<Output> {
    pub fn from_operator<I: 'static, O: 'static>(
        operator: StandardOperator<I, O>,
    ) -> JetStreamBuilder<O> {
        JetStreamBuilder {
            operators: Vec::new(),
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

    pub fn get_output_mut(&mut self) -> &mut Sender<O> {
        self.tail.get_output_mut()
    }

    /// add an operator to the end of this stream
    /// and return a new stream where the new operator is last_op
    pub fn then<O2: Data + 'static>(
        mut self,
        mut operator: StandardOperator<O, O2>,
    ) -> JetStreamBuilder<O2> {
        // let (tx, rx) = selective_broadcast::<O>();
        selective_broadcast::link(self.tail.get_output_mut(), operator.get_input_mut());

        let old_tail_f = self.tail.build();
        self.operators.push(old_tail_f);

        JetStreamBuilder {
            operators: self.operators,
            tail: Box::new(operator),
        }
    }

    pub fn build(mut self) -> JetStream {
        let tail = self.tail.build();
        self.operators.push(tail);
        JetStream {
            operators: self.operators,
        }
    }
}

pub struct JetStream {
    operators: Vec<FrontieredOperator>,
}

impl JetStream {
    pub fn into_operators(self) -> Vec<FrontieredOperator> {
        self.operators
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
        let _union_stream = worker.union_n([stream_a, stream_b]);
    }
}
