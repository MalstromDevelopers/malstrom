use indexmap::IndexSet;

use crate::{
    channels::selective_broadcast::Sender,
    snapshot::{Barrier, Load},
    stream::{
        jetstream::JetStreamBuilder,
        operator::{BuildContext, Logic, OperatorBuilder, OperatorContext},
    },
    time::{Epoch, MaybeTime, NoTime},
    Data, DataMessage, MaybeKey, Message, NoData, NoKey, ShutdownMarker, WorkerId,
};

pub trait IntoSource<K, V, T, P> {
    fn into_source(self) -> OperatorBuilder<NoKey, NoData, NoTime, K, V, T, P>;
}

pub trait Source<K, V, T, P> {
    fn source(self, source: impl IntoSource<K, V, T, P>) -> JetStreamBuilder<K, V, T, P>;
}

impl<K, V, T, P> Source<K, V, T, P> for JetStreamBuilder<NoKey, NoData, NoTime, P>
where
    K: MaybeKey,
    V: Data,
    T: MaybeTime,
    P: 'static,
{
    fn source(self, source: impl IntoSource<K, V, T, P>) -> JetStreamBuilder<K, V, T, P> {
        self.then(source.into_source())
    }
}
