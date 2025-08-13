use serde::{de::DeserializeOwned, Serialize};

use crate::{
    operators::sealed::Sealed,
    stream::{OperatorBuilder, StreamBuilder},
    types::{DataMessage, MaybeData, MaybeKey, Message, Timestamp},
};

use super::util::{handle_maybe_late_msg, split_mixed_stream};
/// Intermediate builder for a timestamped stream.
/// Turn this type into a stream by calling
/// `generate_epochs` or `generate_periodic_epochs` on it
#[must_use = "Call `.generate_epochs()`"]
pub struct NeedsEpochs<K, V, T>(pub(super) StreamBuilder<K, V, T>);

/// Generate Epochs for a stream.
pub trait GenerateEpochs<K, V, T>: Sealed {
    /// Generates Epochs from data. This operator takes a function which may create a new epoch for any
    /// DataMessage arriving at this Operator. To not create a new Epoch, the function must return `None`.
    ///
    /// This operator returns two streams, a stream with all on-time message, i.e. message which are not later
    /// *than the previously issued epoch* and a stream with all late message, i.e. messages with a timestamp
    /// lower than the previously issued Epoch.
    ///
    /// **NOTES:**
    /// - The Epoch generated is always issued *after* the given message.
    /// - If the returned epoch is smaller than the previous epoch, it is ignored
    ///
    /// # Example
    ///
    /// ```no_run
    /// use malstrom::operators::{GenerateEpochs, limit_out_of_orderness};
    /// use malstrom::types::NoKey;
    /// use malstrom::stream::StreamBuilder;
    ///
    /// let stream: StreamBuilder<NoKey, String, i64> = todo!();
    /// stream.generate_epochs("limit", limit_out_of_orderness(30));
    /// ```
    fn generate_epochs(
        self,
        name: &str,
        // previously issued epoch and time elapsed since last epoch
        gen: impl FnMut(&DataMessage<K, V, T>, &Option<T>) -> Option<T> + 'static,
    ) -> (StreamBuilder<K, V, T>, StreamBuilder<K, V, T>);
}

impl<K, V, T> GenerateEpochs<K, V, T> for NeedsEpochs<K, V, T>
where
    K: MaybeKey,
    T: Timestamp + Serialize + DeserializeOwned,
    V: MaybeData,
{
    fn generate_epochs(
        self,
        name: &str,
        gen: impl FnMut(&DataMessage<K, V, T>, &Option<T>) -> Option<T> + 'static,
    ) -> (StreamBuilder<K, V, T>, StreamBuilder<K, V, T>) {
        self.0.generate_epochs(name, gen)
    }
}

impl<K, V, T> GenerateEpochs<K, V, T> for StreamBuilder<K, V, T>
where
    K: MaybeKey,
    T: Timestamp + Serialize + DeserializeOwned,
    V: MaybeData,
{
    fn generate_epochs(
        self,
        name: &str,
        mut gen: impl FnMut(&DataMessage<K, V, T>, &Option<T>) -> Option<T> + 'static,
    ) -> (StreamBuilder<K, V, T>, StreamBuilder<K, V, T>) {
        let operator = OperatorBuilder::built_by(name, |build_context| {
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
                            if prev_epoch.as_ref().is_none_or(|prev| *prev < e) {
                                let _ = prev_epoch.insert(e.clone());
                                output.send(Message::Epoch(e))
                            }
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

/// Creates a function for generating Epochs suitable for limiting message out-of-orderness.
///
/// For example when constructing with `limit_out_of_orderness(Duration::from_secs(30))`
/// all messages with a timstamp more 30 seconds below the largest timestamp seen so far will be
/// categorized late due to the epochs emitted.
pub fn limit_out_of_orderness<K, V, T, B>(
    bound: B,
) -> impl FnMut(&DataMessage<K, V, T>, &Option<T>) -> Option<T> + 'static
where
    T: Timestamp + std::ops::Sub<B, Output = T>,
    B: Clone + 'static,
{
    move |msg, last_epoch| {
        let new_epoch = msg.timestamp.clone() - bound.clone();
        match last_epoch {
            Some(le) => {
                // new message more than `bound` ahead of last epoch
                (new_epoch > *le).then_some(new_epoch)
            }
            None => Some(new_epoch),
        }
    }
}
