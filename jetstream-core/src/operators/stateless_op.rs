use crate::{
    channels::selective_broadcast::{Receiver, Sender},
    stream::{JetStreamBuilder, OperatorBuilder},
    types::{Timestamp,
    Data, DataMessage, MaybeKey, Message,}
};

pub trait StatelessOp<K, VI, T> {
    /// A small wrapper around StandardOperator to make allow simpler
    /// implementations of stateless, time-unaware operators like map or filter
    ///
    /// The mapper is only called for data messages, all other messages are passed
    /// along as they are.
    fn stateless_op<VO: Data>(
        self,
        mapper: impl FnMut(DataMessage<K, VI, T>, &mut Sender<K, VO, T>) + 'static,
    ) -> JetStreamBuilder<K, VO, T>;
}

impl<K, VI, T> StatelessOp<K, VI, T> for JetStreamBuilder<K, VI, T>
where
    K: MaybeKey,
    VI: Data,
    T: Timestamp,
{
    fn stateless_op<VO: Data>(
        self,
        mut mapper: impl FnMut(DataMessage<K, VI, T>, &mut Sender<K, VO, T>) + 'static,
    ) -> JetStreamBuilder<K, VO, T> {
        let op = OperatorBuilder::direct(
            move |input: &mut Receiver<K, VI, T>, output: &mut Sender<K, VO, T>, _ctx| {
                let msg = match input.recv() {
                    Some(x) => x,
                    None => return,
                };
                match msg {
                    Message::Data(d) => mapper(d, output),
                    Message::Interrogate(x) => output.send(Message::Interrogate(x)),
                    Message::Collect(c) => output.send(Message::Collect(c)),
                    Message::Acquire(a) => output.send(Message::Acquire(a)),
                    Message::DropKey(k) => output.send(Message::DropKey(k)),
                    // necessary to convince Rust it is a different generic type now
                    Message::AbsBarrier(b) => output.send(Message::AbsBarrier(b)),
                    // Message::Load(l) => output.send(Message::Load(l)),
                    Message::Rescale(x) => output.send(Message::Rescale(x)),
                    Message::ShutdownMarker(x) => output.send(Message::ShutdownMarker(x)),
                    Message::Epoch(x) => output.send(Message::Epoch(x)),
                };
            },
        );
        self.then(op)
    }
}
