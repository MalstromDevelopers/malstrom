use std::marker::PhantomData;

use crate::{
    channels::operator_io::{Input, Output},
    operators::StreamSink,
    stream::{JetStreamBuilder, OperatorBuilder},
    types::{Data, DataMessage, MaybeKey, Message, NoData, NoKey, NoTime, Timestamp},
};

pub struct StatelessSink<K, V, T, S: StatelessSinkImpl<K, V, T>>(S, PhantomData<(K, V, T)>);
impl<K, V, T, S> StatelessSink<K, V, T, S>
where
    S: StatelessSinkImpl<K, V, T>,
{
    pub fn new(sink: S) -> Self {
        Self(sink, PhantomData)
    }
}

pub trait StatelessSinkImpl<K, V, T>: 'static {
    fn sink(&mut self, msg: DataMessage<K, V, T>);

    /// Suspend this partition.
    /// Suspend means the execution will be halted, but could continue later.
    /// Use this method to clean up any recources like external connections or
    /// file handles
    fn suspend(&mut self) -> () {}
}

impl<K, V, T, S> StreamSink<K, V, T> for StatelessSink<K, V, T, S>
where
    K: MaybeKey,
    V: Data,
    T: Timestamp,
    S: StatelessSinkImpl<K, V, T>,
{
    fn consume_stream(mut self, name: &str, builder: JetStreamBuilder<K, V, T>) -> () {
        builder
            .then(OperatorBuilder::direct(
                name,
                move |input: &mut Input<K, V, T>,
                      _output: &mut Output<NoKey, NoData, NoTime>,
                      _ctx| {
                    if let Some(msg) = input.recv() {
                        match msg {
                            Message::Data(d) => self.0.sink(d),
                            Message::SuspendMarker(_s) => self.0.suspend(),
                            _ => (),
                        }
                    }
                },
            ));
    }
}
