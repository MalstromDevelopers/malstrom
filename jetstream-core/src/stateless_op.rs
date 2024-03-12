use crate::{
    channels::selective_broadcast::{Receiver, Sender},
    snapshot::PersistenceBackend,
    stream::{jetstream::JetStreamBuilder, operator::OperatorBuilder},
    time::Timestamp,
    Data, DataMessage, MaybeKey, Message,
};

/// A small wrapper around StandardOperator to make allow simpler
/// implementations of stateless, time-unaware operators like map or filter
pub trait StatelessOp<K, VI, T, P> {
    fn stateless_op<VO: Data>(
        self,
        mapper: impl FnMut(DataMessage<K, VI, T>, &mut Sender<K, VO, T, P>) + 'static,
    ) -> JetStreamBuilder<K, VO, T, P>;
}

impl<K, VI, T, P> StatelessOp<K, VI, T, P> for JetStreamBuilder<K, VI, T, P>
where
    K: MaybeKey,
    VI: Data,
    T: Timestamp,
    P: PersistenceBackend,
{
    fn stateless_op<VO: Data>(
        self,
        mut mapper: impl FnMut(DataMessage<K, VI, T>, &mut Sender<K, VO, T, P>) + 'static,
    ) -> JetStreamBuilder<K, VO, T, P> {
        let op = OperatorBuilder::direct(
            move |input: &mut Receiver<K, VI, T, P>, output: &mut Sender<K, VO, T, P>, _ctx| {
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
                    Message::Load(l) => output.send(Message::Load(l)),
                    Message::ScaleAddWorker(x) => output.send(Message::ScaleAddWorker(x)),
                    Message::ScaleRemoveWorker(x) => output.send(Message::ScaleRemoveWorker(x)),
                    Message::ShutdownMarker(x) => output.send(Message::ShutdownMarker(x)),
                    Message::Epoch(x) => output.send(Message::Epoch(x)),
                };
            },
        );
        self.then(op)
    }
}
