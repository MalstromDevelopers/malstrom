use std::{process::Output, marker::PhantomData};

use crate::{
    channels::selective_broadcast::{self, Sender},
    frontier::Probe,
    snapshot::{
        backend::{NoState, PersistenceBackend, State},
        barrier::BarrierData,
    },
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
pub struct NoData;

pub struct JetStreamEmpty<P: PersistenceBackend>{
    _p: PhantomData<P>
}

impl<P> JetStreamEmpty<P>
where P: PersistenceBackend {
    pub fn new() -> Self {
        Self {_p: PhantomData}
    }
    /// Add a datasource to a stream which has no data in it
    pub fn source<O: Data>(
        self,
        mut source_func: impl FnMut(&mut FrontierHandle) -> Option<O> + 'static,
    ) -> JetStreamBuilder<O, P> {
        let operator =
            StandardOperator::<NoData, O, NoState>::new(move |input, frontier_handle, _| {
                source_func(frontier_handle)
            });
        JetStreamBuilder {
            operators: Vec::new(),
            tail: Box::new(operator),
        }
    }
}

pub struct JetStreamBuilder<Output, P: PersistenceBackend> {
    operators: Vec<FrontieredOperator<P>>,
    // these are probes for every operator in operators
    tail: Box<dyn AppendableOperator<Output, P>>,
}

impl<P> JetStreamBuilder<Output, P>
where
    P: PersistenceBackend,
{
    pub fn from_operator<I: 'static, O: Data + 'static, S: State>(
        operator: StandardOperator<I, O, S>,
    ) -> JetStreamBuilder<O, P> {
        JetStreamBuilder {
            operators: Vec::new(),
            tail: Box::new(operator),
        }
    }
}

impl<O, P> JetStreamBuilder<O, P>
where
    O: Data,
    P: PersistenceBackend,
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
    pub fn then<O2: Data + 'static, S: State>(
        mut self,
        mut operator: StandardOperator<O, O2, S>,
    ) -> JetStreamBuilder<O2, P> {
        // let (tx, rx) = selective_broadcast::<O>();
        selective_broadcast::link(self.tail.get_output_mut(), operator.get_input_mut());

        let old_tail_f = self.tail.build();
        self.operators.push(old_tail_f);

        JetStreamBuilder {
            operators: self.operators,
            tail: Box::new(operator),
        }
    }

    pub fn build(mut self) -> JetStream<P> {
        let tail = self.tail.build();
        self.operators.push(tail);
        JetStream {
            operators: self.operators,
        }
    }
}

pub struct JetStream<P: PersistenceBackend> {
    operators: Vec<FrontieredOperator<P>>,
}

impl<P> JetStream<P>
where
    P: PersistenceBackend,
{
    pub fn into_operators(self) -> Vec<FrontieredOperator<P>> {
        self.operators
    }
}

// #[cfg(test)]
// mod tests {
//     use crate::worker::Worker;

//     // Note this useful idiom: importing names from outer (for mod tests) scope.
//     use super::*;
//     use pretty_assertions::assert_eq;

//     #[test]
//     fn test_its_way_too_late() {
//         let mut worker = Worker::new();
//         let source_a = |_: &mut FrontierHandle| Some(rand::random::<f64>());
//         let source_b = |_: &mut FrontierHandle| Some(rand::random::<f64>());

//         let stream_a = JetStreamEmpty::new().source(source_a);
//         let stream_b = JetStreamEmpty::new().source(source_b);
//         let _union_stream = worker.union_n([stream_a, stream_b]);
//     }
// }
