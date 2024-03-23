use std::time::{Duration, SystemTime};

use serde::{de::DeserializeOwned, Serialize};

use crate::{
    snapshot::PersistenceBackend,
    stream::{jetstream::JetStreamBuilder, operator::OperatorBuilder},
    time::{Epoch, NoTime, Timestamp},
    MaybeData, MaybeKey, Message, Worker,
};

use super::{
    util::{handle_maybe_late_msg, split_mixed_stream},
    NeedsEpochs,
};

pub trait PeriodicEpochs<K, V, T, P> {
    // this trait works on the stream directly as well
    // as on the NeedsEpochs builder.
    // I <3 Rust

    fn generate_periodic_epochs(
        self,
        worker: &mut Worker<P>,
        // previously issued epoch and sys time elapsed since last epoch
        gen: impl FnMut(&Option<(&T, Duration)>) -> Option<T> + 'static,
    ) -> (
        JetStreamBuilder<K, V, T, P>,
        JetStreamBuilder<K, V, NoTime, P>,
    );
}

impl<K, V, T, P> PeriodicEpochs<K, V, T, P> for NeedsEpochs<K, V, T, P>
where
    K: MaybeKey,
    V: MaybeData,
    T: Timestamp + Serialize + DeserializeOwned,
    P: PersistenceBackend,
{
    fn generate_periodic_epochs(
        self,
        worker: &mut Worker<P>,
        // previously issued epoch and sys time elapsed since last epoch
        gen: impl FnMut(&Option<(&T, Duration)>) -> Option<T> + 'static,
    ) -> (
        JetStreamBuilder<K, V, T, P>,
        JetStreamBuilder<K, V, NoTime, P>,
    ) {
        self.0.generate_periodic_epochs(worker, gen)
    }
}

impl<K, V, T, P> PeriodicEpochs<K, V, T, P> for JetStreamBuilder<K, V, T, P>
where
    K: MaybeKey,
    V: MaybeData,
    T: Timestamp + Serialize + DeserializeOwned,
    P: PersistenceBackend,
{
    fn generate_periodic_epochs(
        self,
        worker: &mut Worker<P>,
        // previously issued epoch and sys time elapsed since last epoch
        mut gen: impl FnMut(&Option<(&T, Duration)>) -> Option<T> + 'static,
    ) -> (
        JetStreamBuilder<K, V, T, P>,
        JetStreamBuilder<K, V, NoTime, P>,
    ) {
        let operator = OperatorBuilder::built_by(move |build_context| {
            let mut state: Option<(T, SystemTime)> = build_context.load_state().unwrap_or_default();
            move |input, output, ctx| {
                // check if the user function want's to create an epoch
                let now = SystemTime::now();
                let elapsed = state
                    .as_ref()
                    .map(|(t, i)| (t, now.duration_since(*i).unwrap_or_default()));
                if let Some(e) = gen(&elapsed) {
                    output.send(Message::Epoch(Epoch::new(e.clone())));
                    state.insert((e, now));
                }
                if let Some(msg) = input.recv() {
                    match msg {
                        Message::AbsBarrier(mut b) => {
                            b.persist(&state, &ctx.operator_id);
                            output.send(Message::AbsBarrier(b))
                        }
                        Message::Epoch(_) => (),
                        Message::Data(d) => {
                            handle_maybe_late_msg(state.as_ref().map(|x| &x.0), d, output);
                        }
                        Message::Interrogate(x) => output.send(Message::Interrogate(x)),
                        Message::Collect(c) => output.send(Message::Collect(c)),
                        Message::Acquire(a) => output.send(Message::Acquire(a)),
                        Message::DropKey(k) => output.send(Message::DropKey(k)),
                        // Message::Load(l) => todo!(),
                        Message::ScaleAddWorker(x) => output.send(Message::ScaleAddWorker(x)),
                        Message::ScaleRemoveWorker(x) => output.send(Message::ScaleRemoveWorker(x)),
                        Message::ShutdownMarker(x) => output.send(Message::ShutdownMarker(x)),
                    }
                }
            }
        });
        let mixed = self.then(operator);
        split_mixed_stream(mixed, worker)
    }
}
