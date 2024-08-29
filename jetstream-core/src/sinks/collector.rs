use crate::{
    operators::sink::{IntoSink, IntoSinkFull}, stream::operator::OperatorBuilder, test::VecCollector, time::{MaybeTime, NoTime, Timestamp}, Data, DataMessage, MaybeData, MaybeKey, Message, NoData, NoKey
};

impl<K, V, T> IntoSink<K, V, T> for VecCollector<DataMessage<K, V, T>>
where
    K: MaybeKey,
    V: Data,
    T: MaybeTime,
{
    fn into_sink(self) -> OperatorBuilder<K, V, T, K, NoData, T> {
        OperatorBuilder::direct(move |input, output, _ctx| {
            if let Some(msg) = input.recv() {
                match msg {
                    Message::Data(x) => self.give(x),
                    Message::Epoch(x) => output.send(Message::Epoch(x)),
                    Message::AbsBarrier(x) => output.send(Message::AbsBarrier(x)),
                    // Message::Load(x) => output.send(Message::Load(x)),
                    Message::Rescale(x) => output.send(Message::Rescale(x)),
                    Message::ShutdownMarker(x) => output.send(Message::ShutdownMarker(x)),
                    Message::Interrogate(x) => output.send(Message::Interrogate(x)),
                    Message::Collect(x) => output.send(Message::Collect(x)),
                    Message::Acquire(x) => output.send(Message::Acquire(x)),
                    Message::DropKey(x) => output.send(Message::DropKey(x)),
                }
            }
        })
    }
}

impl<K, V, T> IntoSinkFull<K, V, T> for VecCollector<Message<K, V, T>>
where
    K: MaybeKey,
    V: MaybeData,
    T: MaybeTime,
{
    fn into_sink_full(self) -> OperatorBuilder<K, V, T, NoKey, NoData, NoTime> {
        OperatorBuilder::direct(move |input, _, _ctx| {
            if let Some(msg) = input.recv() {
                self.give(msg)
            }
        })
    }
}
