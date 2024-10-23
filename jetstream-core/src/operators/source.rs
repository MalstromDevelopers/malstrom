use crate::{
    stream::{JetStreamBuilder, OperatorBuilder},
    types::{Data, MaybeKey, NoData, NoKey, NoTime, Timestamp},
};

pub trait IntoSource<K, V, T> {
    fn into_source(self) -> OperatorBuilder<NoKey, NoData, NoTime, K, V, T>;
}

pub trait Source<K, V, T> {
    fn source(self, source: impl IntoSource<K, V, T>) -> JetStreamBuilder<K, V, T>;
}

impl<K, V, T> Source<K, V, T> for JetStreamBuilder<NoKey, NoData, NoTime>
where
    K: MaybeKey,
    V: Data,
    T: Timestamp,
{
    fn source(self, source: impl IntoSource<K, V, T>) -> JetStreamBuilder<K, V, T> {
        self.then(source.into_source())
    }
}
