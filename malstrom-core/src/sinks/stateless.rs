use std::marker::PhantomData;

use crate::{
    channels::operator_io::{Input, Output},
    operators::StreamSink,
    stream::{Logic, Malstrom as _, Operator, OperatorContext, StreamBuilder},
    types::{Data, DataMessage, Kvt, MaybeKey, Message, NoData, NoKey, NoTime, Timestamp},
};

/// A sink emitting records not hold any state (or only ephemeral state)
pub struct StatelessSink<In: Kvt, SinkImpl: StatelessSinkImpl<In>> {
    sink_impl: SinkImpl,
    _in_type: PhantomData<In>,
}

impl<In, SinkImpl> StatelessSink<In, SinkImpl>
where
    SinkImpl: StatelessSinkImpl<In>,
    In: Kvt,
{
    /// Create a new stateless sink by wrapping a sink implementation
    pub fn new(sink: SinkImpl) -> Self {
        Self {
            sink_impl: sink,
            _in_type: PhantomData,
        }
    }
}

/// Implementation of a stateless stream sink
pub trait StatelessSinkImpl<M: Kvt>: 'static {
    /// Emit a single record
    fn sink(&mut self, msg: DataMessage<M>);
}

impl<M, S> StreamSink<M> for StatelessSink<M, S>
where
    M: Kvt,
    S: StatelessSinkImpl<M>,
{
    fn consume_stream(self, name: &str, builder: StreamBuilder<M>) {
        builder.then(Operator::direct(name.into(), self));
    }
}

impl<M, S> Logic<M, ()> for StatelessSink<M, S>
where
    M: Kvt,
    S: StatelessSinkImpl<M>,
{
    async fn apply(
        &mut self,
        input: &mut Input<M>,
        output: &mut Output<()>,
        ctx: &mut OperatorContext,
    ) {
        if let Message::Data(d) = input.recv().await {
            self.sink_impl.sink(d);
        }
    }
}
