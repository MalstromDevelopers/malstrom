use crate::{
    stream::{
        jetstream::JetStreamBuilder,
        operator::{OperatorBuilder},
    },
    time::{MaybeTime},
    Data, MaybeKey, NoData,
};

pub trait IntoSink<K, V, T, P> {
    fn into_sink(self) -> OperatorBuilder<K, V, T, K, NoData, T, P>;
}

pub trait Sink<K, V, T, P> {
    fn sink(self, sink: impl IntoSink<K, V, T, P>) -> JetStreamBuilder<K, NoData, T, P>;
}

impl<K, V, T, P> Sink<K, V, T, P> for JetStreamBuilder<K, V, T, P>
where
    K: MaybeKey,
    V: Data,
    T: MaybeTime,
    P: 'static,
{
    fn sink(self, sink: impl IntoSink<K, V, T, P>) -> JetStreamBuilder<K, NoData, T, P> {
        self.then(sink.into_sink())
    }
}
