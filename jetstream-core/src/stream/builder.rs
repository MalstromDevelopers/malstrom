//! Builder for datastreams

use std::{iter, rc::Rc, sync::Mutex};

use super::operator::{AppendableOperator, BuildableOperator, OperatorBuilder};
use crate::{
    channels::selective_broadcast::{self, Sender},
    runtime::{split_n, union, InnerRuntimeBuilder},
    types::{Data, MaybeKey, MaybeTime, OperatorPartitioner},
};

#[must_use = "Call .finish() on a stream to finalize it"]
pub struct JetStreamBuilder<K, V, T> {
    operators: Vec<Box<dyn BuildableOperator>>,
    tail: Box<dyn AppendableOperator<K, V, T>>,
    // the runtime this stream is registered to
    runtime: Rc<Mutex<InnerRuntimeBuilder>>,
}

impl<K, V, T> JetStreamBuilder<K, V, T>
where
    K: MaybeKey,
    V: Data,
    T: MaybeTime,
{
    pub(crate) fn from_operator<KI: MaybeKey, VI: Data, TI: MaybeTime>(
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
    T: MaybeTime,
{
    pub fn get_output_mut(&mut self) -> &mut Sender<K, V, T> {
        self.tail.get_output_mut()
    }

    /// add an operator to the end of this stream
    /// and return a new stream where the new operator is last_op
    pub fn then<KO: MaybeKey, VO: Data, TO: MaybeTime>(
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

    /// Add a label to the last operator in this stream.
    /// This can be useful when tracing operator execution
    pub fn label(mut self, label: &str) -> Self {
        self.tail.label(label.into());
        self
    }

    pub fn finish(self) {
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
