use super::operator::{AppendableOperator, FrontieredOperator, StandardOperator};
use crate::{
    channels::selective_broadcast::{self, Sender},
    snapshot::PersistenceBackend,
    time::Timestamp,
    Data, Key,
};

pub struct JetStreamBuilder<K, V, T, P> {
    operators: Vec<FrontieredOperator>,
    // these are probes for every operator in operators
    tail: Box<dyn AppendableOperator<K, V, T, P>>,
}

impl<K, V, T, P> JetStreamBuilder<K, V, T, P>
where
    K: Key,
    V: Data,
    T: Timestamp,
    P: PersistenceBackend,
{
    pub(crate) fn from_operator<KI: Key, VI: Data, TI: Timestamp>(
        operator: StandardOperator<KI, VI, TI, K, V, T, P>,
    ) -> JetStreamBuilder<K, V, T, P> {
        JetStreamBuilder {
            operators: Vec::new(),
            tail: Box::new(operator),
        }
    }
}

impl<K, V, T, P> JetStreamBuilder<K, V, T, P>
where
    K: Key,
    V: Data,
    T: Timestamp,
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

    // pub fn new() -> JetStreamBuilder<NoKey, NoData, NoTime, P> {
    //     JetStreamBuilder::from_operator(StandardOperator::new(
    //         |_input: &mut Receiver<NoKey, NoData, NoTime, P>, _output, ctx: &mut OperatorContext| {
    //             ctx.frontier.advance_to(Timestamp::MAX)
    //         },
    //     ))
    // }

    pub fn get_output_mut(&mut self) -> &mut Sender<K, V, T, P> {
        self.tail.get_output_mut()
    }

    /// add an operator to the end of this stream
    /// and return a new stream where the new operator is last_op
    pub fn then<KO: Key, VO: Data, TO: Timestamp>(
        mut self,
        mut operator: StandardOperator<K, V, T, KO, VO, TO, P>,
    ) -> JetStreamBuilder<KO, VO, TO, P> {
        // let (tx, rx) = selective_broadcast::<O>();
        selective_broadcast::link(self.tail.get_output_mut(), operator.get_input_mut());

        let old_tail_f = self.tail.build();
        self.operators.push(old_tail_f);

        JetStreamBuilder {
            operators: self.operators,
            tail: Box::new(operator),
        }
    }

    pub(crate) fn build(mut self) -> JetStream {
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
