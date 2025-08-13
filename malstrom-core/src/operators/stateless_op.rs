use crate::{
    channels::operator_io::{Input, Output},
    stream::{OperatorBuilder, StreamBuilder},
    types::{Data, DataMessage, MaybeData, MaybeKey, MaybeTime, Message, Timestamp},
};

/// A custom stateless operator for Malstrom streams
pub trait StatelessLogic<K, VI, T, VO>: 'static {
    /// Return Some to retain the key-state and None to discard it
    fn on_data(&mut self, msg: DataMessage<K, VI, T>, output: &mut Output<K, VO, T>);

    /// Handle an incoming epoch. The default implementation is a no-op
    fn on_epoch(&mut self, _epoch: &T, _output: &mut Output<K, VO, T>) {}
}

impl<X, K, VI, T, VO> StatelessLogic<K, VI, T, VO> for X
where
    X: FnMut(DataMessage<K, VI, T>, &mut Output<K, VO, T>) + 'static,
    K: MaybeKey,
    VO: MaybeData,
    T: MaybeTime,
{
    fn on_data(&mut self, msg: DataMessage<K, VI, T>, output: &mut Output<K, VO, T>) {
        self(msg, output);
    }
}

/// Add a custom stateless operator to the stream. See [StatelessLogic] for how to implement a
/// custom stateless operator
pub trait StatelessOp<K, VI, T>: super::sealed::Sealed {
    /// A small wrapper around StandardOperator to make allow simpler
    /// implementations of stateless, time-unaware operators like map or filter
    ///
    /// The mapper is only called for data messages, all other messages are passed
    /// along as they are.
    fn stateless_op<VO: Data>(
        self,
        name: &str,
        logic: impl StatelessLogic<K, VI, T, VO>,
    ) -> StreamBuilder<K, VO, T>;
}

impl<K, VI, T> StatelessOp<K, VI, T> for StreamBuilder<K, VI, T>
where
    K: MaybeKey,
    VI: Data,
    T: Timestamp,
{
    fn stateless_op<VO: Data>(
        self,
        name: &str,
        mut logic: impl StatelessLogic<K, VI, T, VO>,
    ) -> StreamBuilder<K, VO, T> {
        let op = OperatorBuilder::direct(
            name,
            move |input: &mut Input<K, VI, T>, output: &mut Output<K, VO, T>, _ctx| {
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
