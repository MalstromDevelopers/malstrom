use std::marker::PhantomData;

use serde::{Serialize, de::DeserializeOwned};
use tracing::warn;

use crate::{
    msg,
    operators::{StatefulLogic, time::assign_timestamps::OnTimeLate},
    stream::{Logic, LogicBuilder, Malstrom as _, Operator, SafeLogic, StreamBuilder},
    types::{DataMessage, Key, Kvt, MaybeData, MaybeKey, Message, Sealed, Timestamp},
};

use super::util::{handle_maybe_late_msg, split_mixed_stream};
/// Intermediate builder for a timestamped stream.
/// Turn this type into a stream by calling
/// `generate_epochs` or `generate_periodic_epochs` on it
#[must_use = "Call `.generate_epochs()`"]
pub struct NeedsEpochs<Msg: Kvt>(pub(super) StreamBuilder<Msg>);
impl<Msg: Kvt> Sealed for NeedsEpochs<Msg> {}

/// Generate Epochs for a stream.
pub trait GenerateEpochs<Msg: Kvt>: Sealed {
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
        name: impl Into<String>,
        // previously issued epoch and time elapsed since last epoch
        generator: impl FnMut(&DataMessage<Msg>, &Option<Msg::Timestamp>) -> Option<Msg::Timestamp>
        + 'static,
    ) -> (StreamBuilder<msg!(Msg)>, StreamBuilder<msg!(Msg)>);
}

impl<Msg> GenerateEpochs<Msg> for NeedsEpochs<Msg>
where
    Msg: Kvt,
    Msg::Timestamp: Timestamp + Serialize + DeserializeOwned,
{
    fn generate_epochs(
        self,
        name: impl Into<String>,
        generator: impl FnMut(&DataMessage<Msg>, &Option<Msg::Timestamp>) -> Option<Msg::Timestamp>
        + 'static,
    ) -> (StreamBuilder<msg!(Msg)>, StreamBuilder<msg!(Msg)>) {
        self.0.generate_epochs(name, generator)
    }
}

impl<Msg> GenerateEpochs<Msg> for StreamBuilder<Msg>
where
    Msg: Kvt,
    Msg::Timestamp: Timestamp + Serialize + DeserializeOwned,
{
    fn generate_epochs(
        self,
        name: impl Into<String>,
        generator: impl FnMut(&DataMessage<Msg>, &Option<Msg::Timestamp>) -> Option<Msg::Timestamp>
        + 'static,
    ) -> (StreamBuilder<msg!(Msg)>, StreamBuilder<msg!(Msg)>) {
        let operator = Operator::built_by(name.into(), GenerateEpochsOpBuilder { generator });
        let mixed: StreamBuilder<(Msg::Key, OnTimeLate<Msg::Value>, Msg::Timestamp)> =
            self.then(operator);
        split_mixed_stream(mixed)
    }
}

struct GenerateEpochsOp<Msg: Kvt, F> {
    generator: F,
    prev_epoch: Option<Msg::Timestamp>,
}

struct GenerateEpochsOpBuilder<F> {
    generator: F,
}

impl<In, Out, F> LogicBuilder<In, Out> for GenerateEpochsOpBuilder<F>
where
    In: Kvt,
    In::Timestamp: Serialize + DeserializeOwned,
    Out: Kvt<Key = In::Key, Value = OnTimeLate<In::Value>, Timestamp = In::Timestamp>,
    F: FnMut(&DataMessage<In>, &Option<In::Timestamp>) -> Option<In::Timestamp> + 'static,
{
    type Logic = GenerateEpochsOp<In, F>;

    async fn build(self, ctx: &mut crate::stream::BuildContext<'_>) -> Self::Logic {
        let prev_epoch: Option<In::Timestamp> = ctx.load_state().await;
        GenerateEpochsOp {
            generator: self.generator,
            prev_epoch,
        }
    }
}

impl<In, Out, F> Logic<In, Out> for GenerateEpochsOp<In, F>
where
    In: Kvt,
    In::Timestamp: Serialize + DeserializeOwned,
    Out: Kvt<Key = In::Key, Value = OnTimeLate<In::Value>, Timestamp = In::Timestamp>,
    F: FnMut(&DataMessage<In>, &Option<In::Timestamp>) -> Option<In::Timestamp> + 'static,
{
    async fn apply(
        &mut self,
        input: &mut crate::channels::operator_io::Input<In>,
        output: &mut crate::channels::operator_io::Output<Out>,
        ctx: &mut crate::stream::OperatorContext<'_>,
    ) {
        if let Some(msg) = input.recv() {
            match msg {
                Message::Data(d) => {
                    let new_epoch = (self.generator)(&d, &self.prev_epoch);
                    // send the message to the late stream if it is later than the previously
                    // issued epoch
                    handle_maybe_late_msg(self.prev_epoch.as_ref(), d, output);

                    self.prev_epoch = match (new_epoch, self.prev_epoch.take()) {
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
                                warn!("Ignoring issued epoch as it is <= previous epoch");
                                Some(y)
                            }
                        }
                    };
                }
                Message::AbsBarrier(mut b) => {
                    b.persist(&self.prev_epoch, &ctx.operator_id);
                    output.send(Message::AbsBarrier(b))
                }
                Message::Epoch(e) => {
                    if self.prev_epoch.as_ref().is_none_or(|prev| *prev < e) {
                        let _ = self.prev_epoch.insert(e.clone());
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
}

/// Creates a function for generating Epochs suitable for limiting message out-of-orderness.
///
/// For example when constructing with `limit_out_of_orderness(Duration::from_secs(30))`
/// all messages with a timstamp more 30 seconds below the largest timestamp seen so far will be
/// categorized late due to the epochs emitted.
pub fn limit_out_of_orderness<Msg, B>(
    bound: B,
) -> impl FnMut(&DataMessage<Msg>, &Option<Msg::Timestamp>) -> Option<Msg::Timestamp> + 'static
where
    Msg: Kvt,
    Msg::Timestamp: Timestamp + std::ops::Sub<B, Output = Msg::Timestamp>,
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
