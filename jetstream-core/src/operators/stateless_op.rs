use crate::{
    channels::selective_broadcast::{Receiver, Sender},
    stream::{JetStreamBuilder, OperatorBuilder},
    types::{Data, DataMessage, MaybeData, MaybeKey, MaybeTime, Message, Timestamp},
};

pub trait StatelessLogic<K, VI, T, VO>: 'static {
    /// Return Some to retain the key-state and None to discard it
    fn on_data(&mut self, msg: DataMessage<K, VI, T>, output: &mut Sender<K, VO, T>) -> ();

    fn on_epoch(&mut self, _epoch: &T, _output: &mut Sender<K, VO, T>) -> () {}
}

impl<X, K, VI, T, VO> StatelessLogic<K, VI, T, VO> for X
where
    X: FnMut(DataMessage<K, VI, T>, &mut Sender<K, VO, T>) -> () + 'static,
    K: MaybeKey,
    VO: MaybeData,
    T: MaybeTime,
{
    fn on_data(&mut self, msg: DataMessage<K, VI, T>, output: &mut Sender<K, VO, T>) -> () {
        self(msg, output);
    }
}

pub trait StatelessOp<K, VI, T> {
    /// A small wrapper around StandardOperator to make allow simpler
    /// implementations of stateless, time-unaware operators like map or filter
    ///
    /// The mapper is only called for data messages, all other messages are passed
    /// along as they are.
    fn stateless_op<VO: Data>(
        self,
        logic: impl StatelessLogic<K, VI, T, VO>,
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
        mut logic: impl StatelessLogic<K, VI, T, VO>,
    ) -> JetStreamBuilder<K, VO, T> {
        let op = OperatorBuilder::direct(
            move |input: &mut Receiver<K, VI, T>, output: &mut Sender<K, VO, T>, _ctx| {
                let msg = match input.recv() {
                    Some(x) => x,
                    None => return,
                };
                match msg {
                    Message::Data(d) => logic.on_data(d, output),
                    Message::Interrogate(x) => output.send(Message::Interrogate(x)),
                    Message::Collect(c) => output.send(Message::Collect(c)),
                    Message::Acquire(a) => output.send(Message::Acquire(a)),
                    // necessary to convince Rust it is a different generic type now
                    Message::AbsBarrier(b) => output.send(Message::AbsBarrier(b)),
                    // Message::Load(l) => output.send(Message::Load(l)),
                    Message::Rescale(x) => output.send(Message::Rescale(x)),
                    Message::SuspendMarker(x) => output.send(Message::SuspendMarker(x)),
                    Message::Epoch(x) => {
                        logic.on_epoch(&x, output);
                        output.send(Message::Epoch(x))
                    }
                };
            },
        );
        self.then(op)
    }
}
