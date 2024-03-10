pub trait Timestamp: PartialOrd + Clone + 'static {
    const MAX: Self;
    const MIN: Self;
}
/// An Epoch is a special marker message. It indicates that no following message
/// with this key will have a time <= timestamp.
#[derive(Debug, Clone)]
pub struct Epoch<T> {
    timestamp: T
}

#[derive(Clone)]
pub struct NoTime;
impl Timestamp for NoTime {
    const MAX: Self = NoTime;
    const MIN: Self = NoTime;
}
impl PartialOrd for NoTime{
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        None
    }
}
impl PartialEq for NoTime{
    fn eq(&self, other: &Self) -> bool {
        true
    }
}

macro_rules! timestamp_impl {
    ($t:ty) => {
        impl Timestamp for $t {
            const MAX: $t = <$t>::MAX;
            const MIN: $t = <$t>::MIN;
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

timestamp_impl!(f64);
timestamp_impl!(f32);

use std::collections::HashMap;

use itertools::Itertools;

use crate::{
    channels::selective_broadcast::{Receiver, Sender}, snapshot::PersistenceBackend, stream::{
        jetstream::JetStreamBuilder,
        operator::StandardOperator,
    }, Data, DataMessage, Key, Message, NoKey
};

/// IMPORTANT: The epoch generator can semantically only be implemented
/// for a stream with no key
pub trait EpochGenerator<V, TI, P> {
    fn epoch_generator<TO: Timestamp>(
        self,
        mapper: impl FnMut(&DataMessage<NoKey, V, TI>) -> TO + 'static,
        generator: impl FnMut(Option<&DataMessage<NoKey, V, TI>>) -> Option<Epoch<TO>> + 'static
    ) -> JetStreamBuilder<NoKey, V, TO, P>;
}

impl<V, TI, P> EpochGenerator<V, TI, P> for JetStreamBuilder<NoKey, V, TI, P>
where
    V: Data,
    TI: Timestamp,
    P: PersistenceBackend
{
    fn epoch_generator<TO: Timestamp>(
        self,
        mut mapper: impl FnMut(&DataMessage<NoKey, V, TI>) -> TO + 'static,
        mut generator: impl FnMut(Option<&DataMessage<NoKey, V, TI>>) -> Option<Epoch<TO>> + 'static
    ) -> JetStreamBuilder<NoKey, V, TO, P> {
        let op = StandardOperator::new(move |input: &mut Receiver<NoKey, V, TI, P>, output: &mut Sender<NoKey, V, TO, P>, ctx| {
            if let Some(msg) = input.recv() {
                match msg {
                    Message::Data(d) => {
                        if let Some(x) = generator(Some(&d)) {
                            output.send(Message::Epoch(x))
                        }
                        let new_ts = mapper(&d);
                        output.send(Message::Data(DataMessage { key: d.key, value: d.value, time: new_ts }))
                        }
                        Message::Interrogate(mut x) => 
                            output.send(Message::Interrogate(x)),
                        Message::Collect(x) => {
                            // x.add_state(ctx.operator_id, state.get(&x.key))
                            output.send(Message::Collect(x))
                        },
                        Message::Acquire(x) => output.send(Message::Acquire(x)),
                        Message::DropKey(x) => output.send(Message::DropKey(x)),
                        // necessary to convince Rust it is a different generic type now
                        Message::AbsBarrier(b) => output.send(Message::AbsBarrier(b)),
                        Message::Load(l) => {
                            output.send(Message::Load(l))
                        },
                        Message::ScaleAddWorker(x) => output.send(Message::ScaleAddWorker(x)),
                        Message::ScaleRemoveWorker(x) => output.send(Message::ScaleRemoveWorker(x)),
                        Message::ShutdownMarker(x) => output.send(Message::ShutdownMarker(x)),
                        Message::Epoch(x) => (),
                }
            } else {
                if let Some(x) = generator(None) {
                    output.send(Message::Epoch(x))
                }
            }
        });
        self.then(op)
    }
}
