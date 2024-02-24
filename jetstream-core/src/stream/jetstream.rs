use super::operator::{AppendableOperator, FrontieredOperator, StandardOperator};
use crate::{
    channels::selective_broadcast::{self, Sender},
    snapshot::PersistenceBackend,
    Data, Key,
};

pub struct JetStreamBuilder<K, T, P> {
    operators: Vec<FrontieredOperator<P>>,
    // these are probes for every operator in operators
    tail: Box<dyn AppendableOperator<K, T, P>>,
}

impl<K, T, P> JetStreamBuilder<K, T, P>
where
    K: Key,
    T: Data,
    P: PersistenceBackend + 'static,
{
    pub(crate) fn from_operator<KI: Key, TI: Data>(
        operator: StandardOperator<KI, TI, K, T, P>,
    ) -> JetStreamBuilder<K, T, P> {
        JetStreamBuilder {
            operators: Vec::new(),
            tail: Box::new(operator),
        }
    }
}

impl<K, T, P> JetStreamBuilder<K, T, P>
where
    K: Key,
    T: Data,
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

    pub fn get_output_mut(&mut self) -> &mut Sender<K, T, P> {
        self.tail.get_output_mut()
    }

    /// add an operator to the end of this stream
    /// and return a new stream where the new operator is last_op
    pub fn then<KO: Key, TO: Data>(
        mut self,
        mut operator: StandardOperator<K, T, KO, TO, P>,
    ) -> JetStreamBuilder<KO, TO, P> {
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
