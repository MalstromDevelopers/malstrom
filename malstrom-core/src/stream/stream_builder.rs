//! Builder for datastreams

use std::{iter, marker::PhantomData, rc::Rc, sync::Mutex};

use crate::{
    channels::operator_io::{Input, Output, link},
    stream::{LogicBuilder, Operator},
    types::{Data, Kvt, MaybeKey, MaybeTime, Sealed},
    worker::InnerRuntimeBuilder,
};

/// The StreamBuilder allows building datastreams by calling operator methods like `.map` or
/// `.filter` on it. The StreamBuilder needs to be finished by dropping it, which will automatically
/// add it to the worker's execution schedule.
pub struct StreamBuilder<M: Kvt> {
    pub(crate) tail: Input<M>,
    // the runtime this stream is registered to
    pub(crate) runtime: Rc<Mutex<InnerRuntimeBuilder>>,
}

impl<M> StreamBuilder<M>
where
    M: Kvt,
{
    /// Get a reference to the runtime this stream belongs to
    pub(crate) fn get_runtime(&self) -> Rc<Mutex<InnerRuntimeBuilder>> {
        Rc::clone(&self.runtime)
    }
}

pub trait Malstrom<M: Kvt>: Sealed {
    fn then<Out: Kvt, B: LogicBuilder<M, Out>>(
        self,
        operator: Operator<M, B, Out>,
    ) -> StreamBuilder<Out>;
}

pub struct InitialStreamBuilder {
    tail: Input<()>,
    // the runtime this stream is registered to
    runtime: Rc<Mutex<InnerRuntimeBuilder>>,
}
impl InitialStreamBuilder {
    pub(crate) fn new(input: Input<()>, runtime: Rc<Mutex<InnerRuntimeBuilder>>) -> Self {
        Self {
            tail: input,
            runtime,
        }
    }
}

impl Malstrom<()> for InitialStreamBuilder {
    fn then<Out: Kvt, B: LogicBuilder<(), Out>>(
        mut self,
        mut operator: Operator<(), B, Out>,
    ) -> StreamBuilder<Out> {
        std::mem::swap(&mut self.tail, operator.get_input_mut());
        let mut new_tail = Input::new_unlinked();
        link(operator.get_output_mut(), &mut new_tail);
        self.runtime.lock().unwrap().add_operator(operator);
        StreamBuilder {
            tail: new_tail,
            runtime: self.runtime,
        }
    }
}

impl<M> Malstrom<M> for StreamBuilder<M>
where
    M: Kvt,
{
    /// add an operator to the end of this stream
    /// and return a new stream where the new operator is last_op
    fn then<Out: Kvt, T: LogicBuilder<M, Out>>(
        mut self,
        mut operator: Operator<M, T, Out>,
    ) -> StreamBuilder<Out> {
        std::mem::swap(&mut self.tail, operator.get_input_mut());
        let mut new_tail = Input::new_unlinked();
        link(operator.get_output_mut(), &mut new_tail);
        self.runtime.lock().unwrap().add_operator(operator);
        StreamBuilder {
            tail: new_tail,
            runtime: self.runtime,
        }
    }
}
