use crate::{
    stream::{jetstream::JetStreamBuilder, operator::OperatorBuilder},
    time::{MaybeTime, NoTime},
    Data, MaybeData, MaybeKey, NoData, NoKey,
};

/// The Void operator will drop all (yes ALL) messages it receives
/// **including system messages**.
/// This is generally only useful to end a stream, as to not keep any items
/// around, that would never be processed again.
pub(crate) trait Void<K, V, T> {
    fn void(self) -> JetStreamBuilder<NoKey, NoData, NoTime>;
}

impl<K, V, T> Void<K, V, T> for JetStreamBuilder<K, V, T>
where
    K: MaybeKey,
    V: MaybeData,
    T: MaybeTime,
{
    fn void(self) -> JetStreamBuilder<NoKey, NoData, NoTime> {
        self.then(OperatorBuilder::direct(|input, _output, _ctx| {
            let _ = input.recv();
        }))
    }
}
