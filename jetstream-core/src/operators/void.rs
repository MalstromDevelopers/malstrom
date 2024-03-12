use crate::{stream::{jetstream::JetStreamBuilder, operator::OperatorBuilder}, time::{MaybeTime, NoTime}, Data, MaybeKey, NoData, NoKey};

/// The Void operator will drop all (yes ALL) messages it receives
/// **including system messages**.
/// This is generally only useful to end a stream, as to not keep any items
/// around, that would never be processed again.
pub trait Void<K, V, T, P> {
    fn void(
        self,
    ) -> JetStreamBuilder<NoKey, NoData, NoTime, P>;
}

impl<K, V, T, P> Void<K, V, T, P> for JetStreamBuilder<K, V, T, P> where K: MaybeKey, V: Data, T: MaybeTime, P: 'static{
    fn void(
        self,
    ) -> JetStreamBuilder<NoKey, NoData, NoTime, P> {
        self.then(OperatorBuilder::direct(|input, _output, _ctx| {
            let _ = input.recv();
        }))
    }
}