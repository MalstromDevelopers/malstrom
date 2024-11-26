use crate::{
    stream::JetStreamBuilder,
    types::{Data, MaybeKey, NoData, NoKey, NoTime, Timestamp},
};

#[diagnostic::on_unimplemented(message = "You might need to wrap this. Try StatefulSource::new")]
pub trait StreamSource<K, V, T> {
    fn into_stream(
        self,
        builder: JetStreamBuilder<NoKey, NoData, NoTime>,
    ) -> JetStreamBuilder<K, V, T>;
}

pub trait Source<K, V, T, S> {
    fn source(self, source: S) -> JetStreamBuilder<K, V, T>;
}

impl<K, V, T, S> Source<K, V, T, S> for JetStreamBuilder<NoKey, NoData, NoTime>
where
    K: MaybeKey,
    V: Data,
    T: Timestamp,
    S: StreamSource<K, V, T>,
{
    fn source(self, source: S) -> JetStreamBuilder<K, V, T> {
        source.into_stream(self)
    }
}
