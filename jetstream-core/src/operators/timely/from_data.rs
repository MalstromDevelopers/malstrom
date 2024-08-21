use serde::{de::DeserializeOwned, Serialize};

use crate::{
    snapshot::PersistenceClient,
    stream::{jetstream::JetStreamBuilder, operator::OperatorBuilder},
    time::{NoTime, Timestamp},
    DataMessage, MaybeData, MaybeKey, Message,
};

use super::{
    util::{handle_maybe_late_msg, split_mixed_stream},
    NeedsEpochs,
};

pub trait GenerateEpochs<K, V, T> {
    /// Generates Epochs from data. This operator takes a function which may create a new epoch for any
    /// DataMessage arriving at this Operator. To not create a new Epoch, the function must return `None`.
    ///
    /// This operator returns two streams, a stream with all on-time message, i.e. message which are not later
    /// *than the previously issued epoch* and an untimed stream with all late message, i.e. messages with a timestamp
    /// lower than the previously issued Epoch.
    ///
    /// **NOTES:**
    /// - The Epoch generated is always issued *after* the given message.
    /// - This Operator removes any incoming epochs
    /// - If the returned epoch is smaller than the previous epoch, it is ignored
    fn generate_epochs(
        self,
        // previously issued epoch and sys time elapsed since last epoch
        gen: impl FnMut(&DataMessage<K, V, T>, &Option<T>) -> Option<T> + 'static,
    ) -> (JetStreamBuilder<K, V, T>, JetStreamBuilder<K, V, NoTime>);
}

impl<K, V, T> GenerateEpochs<K, V, T> for NeedsEpochs<K, V, T>
where
    K: MaybeKey,
    T: Timestamp + Serialize + DeserializeOwned,
    V: MaybeData,
{
    fn generate_epochs(
        self,
        gen: impl FnMut(&DataMessage<K, V, T>, &Option<T>) -> Option<T> + 'static,
    ) -> (JetStreamBuilder<K, V, T>, JetStreamBuilder<K, V, NoTime>) {
        self.0.generate_epochs(gen)
    }
}

impl<K, V, T> GenerateEpochs<K, V, T> for JetStreamBuilder<K, V, T>
where
    K: MaybeKey,
    T: Timestamp + Serialize + DeserializeOwned,
    V: MaybeData,
{
    fn generate_epochs(
        self,
        mut gen: impl FnMut(&DataMessage<K, V, T>, &Option<T>) -> Option<T> + 'static,
    ) -> (JetStreamBuilder<K, V, T>, JetStreamBuilder<K, V, NoTime>) {
        let operator = OperatorBuilder::built_by(|build_context| {
            let mut prev_epoch: Option<T> = build_context.load_state();

            move |input, output, ctx| {
                if let Some(msg) = input.recv() {
                    match msg {
                        Message::Data(d) => {
                            let new_epoch = gen(&d, &prev_epoch);
                            // send the message to the late stream if it is later than the previously
                            // issued epoch
                            handle_maybe_late_msg(prev_epoch.as_ref(), d, output);

                            prev_epoch = match (new_epoch, prev_epoch.take()) {
                                (None, None) => None,
                                (None, Some(x)) => Some(x),
                                (Some(x), None) => {
                                    output.send(Message::Epoch(x.clone()));
                                    Some(x)
                                }
                                (Some(x), Some(y)) => {
                                    if x > y {
                                        {
                                            output.send(Message::Epoch(x.clone()));
                                            Some(x)
                                        }
                                    } else {
                                        Some(y)
                                    }
                                }
                            };
                        }
                        Message::AbsBarrier(mut b) => {
                            b.persist(&prev_epoch, &ctx.operator_id);
                            output.send(Message::AbsBarrier(b))
                        }
                        Message::Epoch(e) => {
                            if prev_epoch.as_ref().map_or(true, |prev| *prev < e) {
                                let _ = prev_epoch.insert(e.clone());
                                output.send(Message::Epoch(e))
                            }
                        }
                        Message::Interrogate(x) => output.send(Message::Interrogate(x)),
                        Message::Collect(c) => output.send(Message::Collect(c)),
                        Message::Acquire(a) => output.send(Message::Acquire(a)),
                        Message::DropKey(k) => output.send(Message::DropKey(k)),
                        // Message::Load(l) => todo!(),
                        Message::Rescale(x) => output.send(Message::Rescale(x)),
                        Message::ShutdownMarker(x) => output.send(Message::ShutdownMarker(x)),
                    }
                }
            }
        });
        let mixed = self.then(operator);
        split_mixed_stream(mixed)
    }
}
