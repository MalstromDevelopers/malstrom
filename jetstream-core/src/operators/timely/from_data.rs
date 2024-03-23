use std::time::{Duration, Instant};

use serde::{de::DeserializeOwned, Serialize};

use crate::{
    snapshot::PersistenceBackend,
    stream::{jetstream::JetStreamBuilder, operator::OperatorBuilder},
    time::{Epoch, NoTime, Timestamp},
    DataMessage, MaybeData, MaybeKey, Message, Worker,
};

use super::{
    util::{handle_maybe_late_msg, split_mixed_stream},
    NeedsEpochs,
};

pub trait GenerateEpochs<K, V, T, P> {
    /// Generates Epochs from data. This operator takes a function which may create a new epoch for any
    /// DataMessage arriving at this Operator. To not create a new Epoch, the function must return `None`.
    ///
    /// This operator returns two streams, a stream with all on-time message, i.e. message which are not later
    /// *than the previously issued epoch* and an untimed stream with all late message, i.e. messages with a timestamp
    /// lower than the previously issued Epoch.
    ///
    /// NOTE: The Epoch generated is always issued *after* the given message.
    /// NOTE: This Operator removes any incoming epochs
    /// NOTE: If the returned epoch is smaller than the previous epoch, it is ignored
    fn generate_epochs(
        self,
        worker: &mut Worker<P>,
        // previously issued epoch and sys time elapsed since last epoch
        gen: impl FnMut(&DataMessage<K, V, T>, &Option<T>) -> Option<T> + 'static,
    ) -> (
        JetStreamBuilder<K, V, T, P>,
        JetStreamBuilder<K, V, NoTime, P>,
    );
}

impl<K, V, T, P> GenerateEpochs<K, V, T, P> for NeedsEpochs<K, V, T, P>
where
    K: MaybeKey,
    T: Timestamp + Serialize + DeserializeOwned,
    V: MaybeData,
    P: PersistenceBackend,
{
    fn generate_epochs(
        self,
        worker: &mut Worker<P>,
        gen: impl FnMut(&DataMessage<K, V, T>, &Option<T>) -> Option<T> + 'static,
    ) -> (
        JetStreamBuilder<K, V, T, P>,
        JetStreamBuilder<K, V, NoTime, P>,
    ) {
        self.0.generate_epochs(worker, gen)
    }
}

impl<K, V, T, P> GenerateEpochs<K, V, T, P> for JetStreamBuilder<K, V, T, P>
where
    K: MaybeKey,
    T: Timestamp + Serialize + DeserializeOwned,
    V: MaybeData,
    P: PersistenceBackend,
{
    fn generate_epochs(
        self,
        worker: &mut Worker<P>,
        mut gen: impl FnMut(&DataMessage<K, V, T>, &Option<T>) -> Option<T> + 'static,
    ) -> (
        JetStreamBuilder<K, V, T, P>,
        JetStreamBuilder<K, V, NoTime, P>,
    ) {
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
                                    output.send(Message::Epoch(Epoch::new(x.clone())));
                                    Some(x)
                                }
                                (Some(x), Some(y)) => {
                                    if x > y {
                                        {
                                            output.send(Message::Epoch(Epoch::new(x.clone())));
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
                        Message::Epoch(_) => (),
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
