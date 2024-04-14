pub trait Timestamp: PartialOrd + Clone + std::fmt::Debug + 'static {
    const MAX: Self;
    const MIN: Self;

    fn merge(&self, other: &Self) -> Self;
}
/// An Epoch is a special marker message. It indicates that no following message
/// with this key will have a time <= timestamp.
// #[derive(Debug, Clone)]
// pub struct Epoch<T> {
//     pub timestamp: T,
// }

// impl<T> Epoch<T> {
//     pub fn new(timestamp: T) -> Self {
//         Self { timestamp }
//     }
// }

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NoTime;

pub trait MaybeTime: Clone + PartialOrd + 'static {
    /// Try to merge two times, returning Some if the
    /// specific type implementing this trait implements
    /// Timestamp and None if it does not
    fn try_merge(&self, other: &Self) -> Option<Self>;
}
impl<T: Timestamp + Clone + 'static> MaybeTime for T {
    fn try_merge(&self, other: &Self) -> Option<Self> {
        Some(self.merge(other))
    }
}
impl MaybeTime for NoTime {
    fn try_merge(&self, other: &Self) -> Option<Self> {
        None
    }
}

impl PartialOrd for NoTime {
    fn partial_cmp(&self, _other: &Self) -> Option<std::cmp::Ordering> {
        None
    }
}
impl PartialEq for NoTime {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

// TODO: Maybe make it so we always give out two streams: OnTime/Late

pub trait TimestampAssigner<K, V, T, TO> {
    fn assign_timestamp(
        &self,
        msg: &DataMessage<K, V, T>,
        last_epoch: &TO,
        ctx: &OperatorContext,
    ) -> TO;

    fn issue_epochs(
        &self,
        msg: &DataMessage<K, V, TO>,
        last_epoch: &TO,
        ctx: &OperatorContext,
    ) -> Option<TO>;
}

macro_rules! timestamp_impl {
    ($t:ty) => {
        impl Timestamp for $t {
            const MAX: $t = <$t>::MAX;
            const MIN: $t = <$t>::MIN;

            fn merge(&self, other: &$t) -> $t {
                *self.min(other)
            }
        }
    };
}

timestamp_impl!(usize);
timestamp_impl!(u8);
timestamp_impl!(u16);
timestamp_impl!(u32);
timestamp_impl!(u64);
timestamp_impl!(u128);

timestamp_impl!(isize);
timestamp_impl!(i8);
timestamp_impl!(i16);
timestamp_impl!(i32);
timestamp_impl!(i64);
timestamp_impl!(i128);

impl Timestamp for f32 {
    const MAX: f32 = f32::MAX;
    const MIN: f32 = f32::MIN;

    fn merge(&self, other: &Self) -> Self {
        self.min(*other)
    }
}

impl Timestamp for f64 {
    const MAX: f64 = f64::MAX;
    const MIN: f64 = f64::MIN;

    fn merge(&self, other: &Self) -> Self {
        self.min(*other)
    }
}

use serde::{Deserialize, Serialize};

use crate::{
    channels::selective_broadcast::{Receiver, Sender},
    snapshot::PersistenceBackend,
    stream::{
        jetstream::JetStreamBuilder,
        operator::{OperatorBuilder, OperatorContext},
    },
    Data, DataMessage, Message, NoKey,
};

/// IMPORTANT: The epoch generator can semantically only be implemented
/// for a stream with no key
pub trait EpochGenerator<V, TI> {
    fn epoch_generator<TO: Timestamp>(
        self,
        mapper: impl FnMut(&DataMessage<NoKey, V, TI>) -> TO + 'static,
        generator: impl FnMut(Option<&DataMessage<NoKey, V, TI>>) -> Option<TO> + 'static,
    ) -> JetStreamBuilder<NoKey, V, TO>;
}

impl<V, TI> EpochGenerator<V, TI> for JetStreamBuilder<NoKey, V, TI>
where
    V: Data,
    TI: Timestamp,
{
    fn epoch_generator<TO: Timestamp>(
        self,
        mut mapper: impl FnMut(&DataMessage<NoKey, V, TI>) -> TO + 'static,
        mut generator: impl FnMut(Option<&DataMessage<NoKey, V, TI>>) -> Option<TO> + 'static,
    ) -> JetStreamBuilder<NoKey, V, TO> {
        let op = OperatorBuilder::direct(
            move |input: &mut Receiver<NoKey, V, TI>, output: &mut Sender<NoKey, V, TO>, _ctx| {
                if let Some(msg) = input.recv() {
                    match msg {
                        Message::Data(d) => {
                            if let Some(x) = generator(Some(&d)) {
                                output.send(Message::Epoch(x))
                            }
                            let new_ts = mapper(&d);
                            output.send(Message::Data(DataMessage {
                                key: d.key,
                                value: d.value,
                                timestamp: new_ts,
                            }))
                        }
                        Message::Interrogate(x) => output.send(Message::Interrogate(x)),
                        Message::Collect(x) => {
                            // x.add_state(ctx.operator_id, state.get(&x.key))
                            output.send(Message::Collect(x))
                        }
                        Message::Acquire(x) => output.send(Message::Acquire(x)),
                        Message::DropKey(x) => output.send(Message::DropKey(x)),
                        // necessary to convince Rust it is a different generic type now
                        Message::AbsBarrier(b) => output.send(Message::AbsBarrier(b)),
                        // Message::Load(l) => output.send(Message::Load(l)),
                        Message::Rescale(x) => output.send(Message::Rescale(x)),
                        Message::ShutdownMarker(x) => output.send(Message::ShutdownMarker(x)),
                        Message::Epoch(_x) => (),
                    }
                } else if let Some(x) = generator(None) {
                    output.send(Message::Epoch(x))
                }
            },
        );
        self.then(op)
    }
}
