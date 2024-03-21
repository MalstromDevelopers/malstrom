use crate::{
    stream::{jetstream::JetStreamBuilder, operator::OperatorBuilder},
    time::{MaybeTime, NoTime},
    Data, MaybeKey, NoData, NoKey,
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
