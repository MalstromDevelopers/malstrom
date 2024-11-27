//! Builder for datastreams

use std::{iter, rc::Rc, sync::Mutex};

use super::operator::{AppendableOperator, BuildableOperator, OperatorBuilder};
use crate::{
    channels::selective_broadcast::{link, Input},
    runtime::{union, InnerRuntimeBuilder},
    types::{Data, MaybeData, MaybeKey, MaybeTime, OperatorPartitioner},
};

#[must_use = "Call .finish() on a stream to finalize it"]
pub struct JetStreamBuilder<K, V, T> {
    operators: Vec<Box<dyn BuildableOperator>>,
    tail: Input<K, V, T>,
    // the runtime this stream is registered to
    runtime: Rc<Mutex<InnerRuntimeBuilder>>,
}

impl<K, V, T> JetStreamBuilder<K, V, T> {
    /// Get a reference to the runtime this stream belongs to
    pub(crate) fn get_runtime(&self) -> Rc<Mutex<InnerRuntimeBuilder>> {
        Rc::clone(&self.runtime)
    }
}

impl<K, V, T> JetStreamBuilder<K, V, T>
where
    K: MaybeKey,
    V: Data,
    T: MaybeTime,
{
    pub(crate) fn from_receiver(
        receiver: Input<K, V, T>,
        runtime: Rc<Mutex<InnerRuntimeBuilder>>,
    ) -> JetStreamBuilder<K, V, T> {
        JetStreamBuilder {
            operators: Vec::new(),
            tail: receiver,
            runtime,
        }
    }
    pub(crate) fn from_operator<KI: MaybeKey, VI: MaybeData, TI: MaybeTime>(
        mut operator: OperatorBuilder<KI, VI, TI, K, V, T>,
        runtime: Rc<Mutex<InnerRuntimeBuilder>>,
    ) -> JetStreamBuilder<K, V, T> {
        let mut receiver = Input::new_unlinked();
        link(operator.get_output_mut(), &mut receiver);
        let operator = Box::new(operator).into_buildable();
        JetStreamBuilder {
            operators: vec![operator],
            tail: receiver,
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

    /// add an operator to the end of this stream
    /// and return a new stream where the new operator is last_op
    pub fn then<KO: MaybeKey, VO: Data, TO: MaybeTime>(
        mut self,
        mut operator: OperatorBuilder<K, V, T, KO, VO, TO>,
    ) -> JetStreamBuilder<KO, VO, TO> {
        // TODO: kinda hacky
        std::mem::swap(&mut self.tail, operator.get_input_mut());
        let mut new_tail = Input::new_unlinked();
        link(operator.get_output_mut(), &mut new_tail);
        self.operators.push(Box::new(operator).into_buildable());

        JetStreamBuilder {
            tail: new_tail,
            operators: self.operators,
            runtime: self.runtime,
        }
    }

    pub(crate) fn into_operators(self) -> Vec<Box<dyn BuildableOperator>> {
        self.operators
    }

    /// Add a label to the last operator in this stream.
    /// This can be useful when tracing operator execution
    // pub fn label(mut self, label: &str) -> Self {
    //     self.tail.label(label.into());
    //     self
    // }

    pub fn finish(self) {
        let rt = self.runtime.clone();
        rt.lock().unwrap().add_operators(self.operators);
    }
    
    pub(crate) fn finish_pop_tail(self) -> Input<K, V, T> {
        let rt = self.runtime.clone();
        rt.lock().unwrap().add_operators(self.operators);
        self.tail
    }

    pub fn union(
        self,
        others: impl Iterator<Item = JetStreamBuilder<K, V, T>>,
    ) -> JetStreamBuilder<K, V, T> {
        let runtime = self.runtime.clone();
        union(runtime, iter::once(self).chain(others))
    }

}
