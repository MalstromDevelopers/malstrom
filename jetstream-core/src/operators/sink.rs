use crate::{
    stream::{JetStreamBuilder, OperatorBuilder},
    types::{Data, MaybeData, MaybeKey, MaybeTime, NoData, NoKey, NoTime},
};

pub trait IntoSink<K, V, T> {
    fn into_sink(self, name: &str) -> OperatorBuilder<K, V, T, K, NoData, T>;
}

pub trait Sink<K, V, T>: super::sealed::Sealed {
    fn sink(self, name: &str, sink: impl IntoSink<K, V, T>) -> JetStreamBuilder<K, NoData, T>;
}

impl<K, V, T> Sink<K, V, T> for JetStreamBuilder<K, V, T>
where
    K: MaybeKey,
    V: Data,
    T: MaybeTime,
{
    fn sink(self, name: &str, sink: impl IntoSink<K, V, T>) -> JetStreamBuilder<K, NoData, T> {
        self.then(sink.into_sink(name))
    }
}

pub trait IntoSinkFull<K, V, T> {
    fn into_sink_full(self, name: &str) -> OperatorBuilder<K, V, T, NoKey, NoData, NoTime>;
}

pub trait SinkFull<K, V, T>: super::sealed::Sealed {
    fn sink_full(self, name: &str, sink: impl IntoSinkFull<K, V, T>)
        -> JetStreamBuilder<NoKey, NoData, NoTime>;
}

impl<K, V, T> SinkFull<K, V, T> for JetStreamBuilder<K, V, T>
where
    K: MaybeKey,
    V: MaybeData,
    T: MaybeTime,
{
    fn sink_full(
        self,
        name: &str,
        sink: impl IntoSinkFull<K, V, T>,
    ) -> JetStreamBuilder<NoKey, NoData, NoTime> {
        self.then(sink.into_sink_full(name))
    }
}
