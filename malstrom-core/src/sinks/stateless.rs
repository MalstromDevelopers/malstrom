use std::marker::PhantomData;

use crate::{
    channels::operator_io::{Input, Output},
    operators::StreamSink,
    stream::{OperatorBuilder, StreamBuilder},
    types::{Data, DataMessage, MaybeKey, Message, NoData, NoKey, NoTime, Timestamp},
};

/// A sink emitting records not hold any state (or only ephemeral state)
pub struct StatelessSink<K, V, T, S: StatelessSinkImpl<K, V, T>>(S, PhantomData<(K, V, T)>);
impl<K, V, T, S> StatelessSink<K, V, T, S>
where
    S: StatelessSinkImpl<K, V, T>,
{
    /// Create a new stateless sink by wrapping a sink implementation
    pub fn new(sink: S) -> Self {
        Self(sink, PhantomData)
    }
}

/// Implementation of a stateless stream sink
pub trait StatelessSinkImpl<K, V, T>: 'static {
    /// Emit a single record
    fn sink(&mut self, msg: DataMessage<K, V, T>);
}

impl<K, V, T, S> StreamSink<K, V, T> for StatelessSink<K, V, T, S>
where
    K: MaybeKey,
    V: Data,
    T: Timestamp,
    S: StatelessSinkImpl<K, V, T>,
{
    fn consume_stream(mut self, name: &str, builder: StreamBuilder<K, V, T>) {
        builder.then(OperatorBuilder::direct(
            name,
            move |input: &mut Input<K, V, T>, _output: &mut Output<NoKey, NoData, NoTime>, _ctx| {
                if let Some(msg) = input.recv() {
                    match msg {
                        Message::Data(d) => self.0.sink(d),
                        _ => (),
                    }
                }
            },
        ));
    }
}
