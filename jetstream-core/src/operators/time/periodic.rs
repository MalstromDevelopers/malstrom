use std::time::{Duration, SystemTime};

use serde::{de::DeserializeOwned, Serialize};

use crate::{
    stream::{JetStreamBuilder, OperatorBuilder},
    types::{MaybeData, MaybeKey, Message, Timestamp},
};

use super::{
    util::{handle_maybe_late_msg, split_mixed_stream},
    NeedsEpochs,
};

pub trait PeriodicEpochs<K, V, T> {
    // this trait works on the stream directly as well
    // as on the NeedsEpochs builder.
    // I <3 Rust

    fn generate_periodic_epochs(
        self,
        // previously issued epoch and sys time elapsed since last epoch
        gen: impl FnMut(&Option<(&T, Duration)>) -> Option<T> + 'static,
    ) -> (JetStreamBuilder<K, V, T>, JetStreamBuilder<K, V, T>);
}

impl<K, V, T> PeriodicEpochs<K, V, T> for NeedsEpochs<K, V, T>
where
    K: MaybeKey,
    V: MaybeData,
    T: Timestamp + Serialize + DeserializeOwned,
{
    fn generate_periodic_epochs(
        self,
        // previously issued epoch and sys time elapsed since last epoch
        gen: impl FnMut(&Option<(&T, Duration)>) -> Option<T> + 'static,
    ) -> (JetStreamBuilder<K, V, T>, JetStreamBuilder<K, V, T>) {
        self.0.generate_periodic_epochs(gen)
    }
}

impl<K, V, T> PeriodicEpochs<K, V, T> for JetStreamBuilder<K, V, T>
where
    K: MaybeKey,
    V: MaybeData,
    T: Timestamp + Serialize + DeserializeOwned,
{
    fn generate_periodic_epochs(
        self,
        // previously issued epoch and sys time elapsed since last epoch
        mut gen: impl FnMut(&Option<(&T, Duration)>) -> Option<T> + 'static,
    ) -> (JetStreamBuilder<K, V, T>, JetStreamBuilder<K, V, T>) {
        let operator = OperatorBuilder::built_by(move |build_context| {
            let mut state: Option<(T, SystemTime)> = build_context.load_state().unwrap_or_default();
            move |input, output, ctx| {
                // check if the user function want's to create an epoch
                let now = SystemTime::now();
                let elapsed = state
                    .as_ref()
                    .map(|(t, i)| (t, now.duration_since(*i).unwrap_or_default()));
                if let Some(e) = gen(&elapsed) {
                    output.send(Message::Epoch(e.clone()));
                    state = Some((e, now));
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
                        // Message::Load(l) => todo!(),
                        Message::Rescale(x) => output.send(Message::Rescale(x)),
                        Message::SuspendMarker(x) => output.send(Message::SuspendMarker(x)),
                    }
                }
            }
        });
        let mixed = self.then(operator);
        split_mixed_stream(mixed)
    }
}
