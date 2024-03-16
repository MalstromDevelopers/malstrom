use crate::{
    channels::selective_broadcast::Sender,
    snapshot::{Barrier, Load},
    stream::{
        jetstream::JetStreamBuilder,
        operator::{BuildContext, OperatorBuilder, OperatorContext},
    },
    time::{Epoch, MaybeTime, NoTime},
    Data, DataMessage, MaybeKey, Message, NoData, NoKey, ShutdownMarker, WorkerId,
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
