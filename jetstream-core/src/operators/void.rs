use crate::{
    channels::selective_broadcast::{Receiver, Sender},
    stream::{JetStreamBuilder, OperatorBuilder, OperatorContext},
    types::{MaybeData, MaybeKey, MaybeTime, NoData, NoKey, NoTime},
};

/// The Void operator will drop all (yes ALL) messages it receives
/// **including system messages**.
/// This is generally only useful to end a stream, as to not keep any items
/// around, that would never be processed.
pub(crate) trait Void<K, V, T> {
    fn void(self) -> JetStreamBuilder<NoKey, NoData, NoTime>;
}

impl<K, V, T> Void<K, V, T> for JetStreamBuilder<K, V, T>
where
    K: MaybeKey,
    V: MaybeData,
    T: MaybeTime,
{
    fn void(self) -> JetStreamBuilder<NoKey, NoData, NoTime> {
        let op = OperatorBuilder::direct(void);
        self.then(op)
    }
}

fn void<K: MaybeKey, V: MaybeData, T: MaybeTime>(
    input: &mut Receiver<K, V, T>,
    _output: &mut Sender<NoKey, NoData, NoTime>,
    _out: &mut OperatorContext,
) {
    input.recv();
}

#[cfg(test)]
mod test {
    use super::*;
    use std::rc::Rc;

    use indexmap::{IndexMap, IndexSet};

    use crate::{
        keyed::distributed::{Acquire, Collect, Interrogate},
        snapshot::{Barrier, NoPersistence},
        testing::OperatorTester,
        types::{DataMessage, Message, RescaleMessage, SuspendMarker},
    };
    /// Simple test, the operator must destroy everything 💀
    #[test]
    fn nothing_comes_out() {
        let mut tester: OperatorTester<i32, i32, i32, NoKey, NoData, NoTime, ()> =
            OperatorTester::built_by(|_| void, 0, 0, 0..1);

        let messages = [
            Message::AbsBarrier(Barrier::new(Box::<NoPersistence>::default())),
            Message::Acquire(Acquire::new(1, IndexMap::new())),
            Message::Collect(Collect::new(1)),
            Message::Data(DataMessage::new(1, 2, 3)),
            Message::Epoch(1),
            Message::Interrogate(Interrogate::new(Rc::new(|_| false))),
            Message::Rescale(RescaleMessage::ScaleAddWorker(IndexSet::new())),
            Message::SuspendMarker(SuspendMarker::default()),
        ];
        for m in messages.into_iter() {
            tester.send_local(m);
            tester.step();
            assert!(tester.recv_local().is_none())
        }
    }
}
