//! Builder for datastreams

use std::{iter, rc::Rc, sync::Mutex};

use super::operator::{AppendableOperator, BuildableOperator, OperatorBuilder};
use crate::{
    channels::operator_io::{link, Input},
    types::{Data, MaybeKey, MaybeTime},
    worker::{union, InnerRuntimeBuilder},
};

/// The StreamBuilder allows building datastreams by calling operator methods like `.map` or
/// `.filter` on it. The StreamBuilder needs to be finished by dropping it, which will automatically
/// add it to the worker's execution schedule.
pub struct StreamBuilder<K, V, T> {
    operators: Vec<Box<dyn BuildableOperator>>,
    tail: Input<K, V, T>,
    // the runtime this stream is registered to
    runtime: Rc<Mutex<InnerRuntimeBuilder>>,
}

impl<K, V, T> StreamBuilder<K, V, T> {
    /// Get a reference to the runtime this stream belongs to
    pub(crate) fn get_runtime(&self) -> Rc<Mutex<InnerRuntimeBuilder>> {
        Rc::clone(&self.runtime)
    }
}

impl<K, V, T> StreamBuilder<K, V, T>
where
    K: MaybeKey,
    V: Data,
    T: MaybeTime,
{
    pub(crate) fn from_receiver(
        receiver: Input<K, V, T>,
        runtime: Rc<Mutex<InnerRuntimeBuilder>>,
    ) -> StreamBuilder<K, V, T> {
        StreamBuilder {
            operators: Vec::new(),
            tail: receiver,
            runtime,
        }
    }
}

impl<K, V, T> StreamBuilder<K, V, T>
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
    ) -> StreamBuilder<KO, VO, TO> {
        // TODO: kinda hacky
        std::mem::swap(&mut self.tail, operator.get_input_mut());
        let mut new_tail = Input::new_unlinked();
        link(operator.get_output_mut(), &mut new_tail);
        self.operators.push(Box::new(operator).into_buildable());

        StreamBuilder {
            tail: new_tail,
            operators: self.operators.drain(..).collect(),
            runtime: Rc::clone(&self.runtime),
        }
    }

    pub(crate) fn finish_pop_tail(mut self) -> Input<K, V, T> {
        let rt = self.runtime.clone();
        #[allow(clippy::unwrap_used)]
        rt.lock().unwrap().add_operators(self.operators.drain(..));
        let mut placeholder = Input::new_unlinked();
        std::mem::swap(&mut placeholder, &mut self.tail);
        placeholder
    }

    /// Combine multiple streams into a single stream of all messages
    pub fn union(
        self,
        others: impl IntoIterator<Item = StreamBuilder<K, V, T>>,
    ) -> StreamBuilder<K, V, T> {
        let runtime = self.runtime.clone();
        union(runtime, iter::once(self).chain(others))
    }
}

impl<K, V, T> Drop for StreamBuilder<K, V, T> {
    fn drop(&mut self) {
        #[allow(clippy::unwrap_used)]
        self.runtime
            .lock()
            .unwrap()
            .add_operators(self.operators.drain(..));
    }
}
