use crate::{
    stream::{JetStreamBuilder, OperatorBuilder}, types::{MaybeTime, NoTime, Data, MaybeData, MaybeKey, NoData, NoKey}
};

pub trait IntoSink<K, V, T> {
    fn into_sink(self) -> OperatorBuilder<K, V, T, K, NoData, T>;
}

pub trait Sink<K, V, T> {
    fn sink(self, sink: impl IntoSink<K, V, T>) -> JetStreamBuilder<K, NoData, T>;
}

impl<K, V, T> Sink<K, V, T> for JetStreamBuilder<K, V, T>
where
    K: MaybeKey,
    V: Data,
    T: MaybeTime,
{
    fn sink(self, sink: impl IntoSink<K, V, T>) -> JetStreamBuilder<K, NoData, T> {
        self.then(sink.into_sink())
    }
}

pub trait IntoSinkFull<K, V, T> {
    fn into_sink_full(self) -> OperatorBuilder<K, V, T, NoKey, NoData, NoTime>;
}

pub trait SinkFull<K, V, T> {
    fn sink_full(self, sink: impl IntoSinkFull<K, V, T>) -> JetStreamBuilder<NoKey, NoData, NoTime>;
}

impl<K, V, T> SinkFull<K, V, T> for JetStreamBuilder<K, V, T>
where
    K: MaybeKey,
    V: MaybeData,
    T: MaybeTime,
{
    fn sink_full(self, sink: impl IntoSinkFull<K, V, T>) -> JetStreamBuilder<NoKey, NoData, NoTime> {
        self.then(sink.into_sink_full())
    }
}
