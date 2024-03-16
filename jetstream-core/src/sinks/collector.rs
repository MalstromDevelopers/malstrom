use std::{rc::Rc, sync::Mutex};

use crate::{
    operators::sink::IntoSink, snapshot::PersistenceBackend, stream::operator::OperatorBuilder,
    test::VecCollector, time::MaybeTime, Data, DataMessage, MaybeKey, Message, NoData,
};

impl<K, V, T, P> IntoSink<K, V, T, P> for VecCollector<DataMessage<K, V, T>>
where
    K: MaybeKey,
    V: Data,
    T: MaybeTime,
    P: PersistenceBackend,
{
    fn into_sink(self) -> OperatorBuilder<K, V, T, K, NoData, T, P> {
        OperatorBuilder::direct(move |input, output, _ctx| {
            if let Some(msg) = input.recv() {
                match msg {
                    Message::Data(x) => self.give(x),
                    Message::Epoch(x) => output.send(Message::Epoch(x)),
                    Message::AbsBarrier(x) => output.send(Message::AbsBarrier(x)),
                    Message::Load(x) => output.send(Message::Load(x)),
                    Message::ScaleRemoveWorker(x) => output.send(Message::ScaleRemoveWorker(x)),
                    Message::ScaleAddWorker(x) => output.send(Message::ScaleAddWorker(x)),
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
