use std::{iter, rc::Rc, sync::Mutex};

use super::operator::{
    pass_through_operator, AppendableOperator, BuildableOperator, OperatorBuilder,
};
use crate::{
    channels::selective_broadcast::{self, Sender},
    time::{NoTime, Timestamp},
    runtime::{split_n, union, InnerRuntimeBuilder},
    Data, MaybeKey, NoData, NoKey, OperatorPartitioner,
};

pub struct JetStreamBuilder<K, V: ?Sized, T> {
    operators: Vec<Box<dyn BuildableOperator>>,
    tail: Box<dyn AppendableOperator<K, V, T>>,
    // the runtime this stream is registered to
    runtime: Rc<Mutex<InnerRuntimeBuilder>>,
}

impl JetStreamBuilder<NoKey, NoData, NoTime> {
    /// Create a new JetStreamBuilder which does not produce any data, keys,
    /// timestamps and does not have any persistence.
    ///
    /// This method is intented for construction streams in unit tests.
    /// For actual deployments you should use [`jetstream::Worker::new_stream`]
    pub fn new_test() -> Self {
        let rt = Rc::new(Mutex::new(InnerRuntimeBuilder::default()));
        JetStreamBuilder::from_operator(pass_through_operator(), rt)
    }
}

impl<K, V, T> JetStreamBuilder<K, V, T>
where
    K: MaybeKey,
    V: Data,
    T: Timestamp,
{
    pub(crate) fn from_operator<KI: MaybeKey, VI: Data, TI: Timestamp>(
        operator: OperatorBuilder<KI, VI, TI, K, V, T>,
        runtime: Rc<Mutex<InnerRuntimeBuilder>>,
    ) -> JetStreamBuilder<K, V, T> {
        JetStreamBuilder {
            operators: Vec::new(),
            tail: Box::new(operator),
            runtime,
        }
    }
}

impl<K, V, T> JetStreamBuilder<K, V, T>
where
    K: MaybeKey,
    V: Data,
    T: Timestamp,
{
    pub fn get_output_mut(&mut self) -> &mut Sender<K, V, T> {
        self.tail.get_output_mut()
    }

    /// add an operator to the end of this stream
    /// and return a new stream where the new operator is last_op
    pub fn then<KO: MaybeKey, VO: Data, TO: Timestamp>(
        mut self,
        mut operator: OperatorBuilder<K, V, T, KO, VO, TO>,
    ) -> JetStreamBuilder<KO, VO, TO> {
        // let (tx, rx) = selective_broadcast::<O>();
        selective_broadcast::link(self.tail.get_output_mut(), operator.get_input_mut());

        let old_tail_f = self.tail.into_buildable();
        self.operators.push(old_tail_f);

        JetStreamBuilder {
            operators: self.operators,
            tail: Box::new(operator),
            runtime: self.runtime,
        }
    }

    pub(crate) fn into_operators(mut self) -> Vec<Box<dyn BuildableOperator>> {
        self.operators.push(self.tail.into_buildable());
        self.operators
    }

    pub(crate) fn operator_count(&self) -> usize {
        // + 1 for the tail
        self.operators.len() + 1
    }

    /// Add a label to the last operator in this stream.
    /// This can be useful when tracing operator execution
    pub fn label(mut self, label: &str) -> Self {
        self.tail.label(label.into());
        self
    }

    pub fn finish(self) -> () {
        let rt = self.runtime.clone();
        rt.lock().unwrap().add_stream(self);
    }

    pub fn union(
        self,
        others: impl Iterator<Item = JetStreamBuilder<K, V, T>>,
    ) -> JetStreamBuilder<K, V, T> {
        let runtime = self.runtime.clone();
        union(runtime, iter::once(self).chain(others))
    }

    pub fn split_n<const N: usize>(
        self,
        partitioner: impl OperatorPartitioner<K, V, T>,
    ) -> [JetStreamBuilder<K, V, T>; N] {
        let runtime = self.runtime.clone();
        split_n(runtime, self, partitioner)
    }
}
