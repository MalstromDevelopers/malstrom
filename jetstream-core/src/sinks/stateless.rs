use std::marker::PhantomData;

use crate::{operators::IntoSink, stream::OperatorBuilder, types::{Data, DataMessage, MaybeKey, MaybeTime, Message, NoData}};

pub struct StatelessSink<K, V, T, S: StatelessSinkImpl<K, V, T>>(S, PhantomData<(K, V, T)>);

impl<K, V, T, S> StatelessSink<K, V, T, S>
where
    S: StatelessSinkImpl<K, V, T>,
{
    pub fn new(source: S) -> Self {
        Self(source, PhantomData)
    }
}
pub trait StatelessSinkImpl<K, V, T>: 'static {
    fn sink_message(&mut self, msg: DataMessage<K, V, T>) -> ();
}

impl<K, V, T, S> IntoSink<K, V, T> for StatelessSink<K, V, T, S>
where
    K: MaybeKey,
    V: Data,
    T: MaybeTime,
    S: StatelessSinkImpl<K, V, T>
{
    fn into_sink(mut self, name: &str) -> OperatorBuilder<K, V, T, K, NoData, T> {
        OperatorBuilder::direct( name, move |input, _out, _ctx| {
            if let Some(Message::Data(msg)) = input.recv() {
                self.0.sink_message(msg);
            }
        })
    }
}
