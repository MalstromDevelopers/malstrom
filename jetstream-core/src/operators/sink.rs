use crate::{
    stream::{jetstream::JetStreamBuilder, operator::OperatorBuilder},
    time::MaybeTime,
    Data, MaybeKey, NoData,
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
