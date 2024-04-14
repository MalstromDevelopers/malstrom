use std::collections::VecDeque;
use std::iter;

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

use crate::snapshot::Barrier;
use crate::stream::operator::Logic;
use crate::DataMessage;
use crate::{
    channels::selective_broadcast::{Receiver, Sender},
    stream::operator::{BuildContext, OperatorContext},
    time::MaybeTime,
    MaybeData, MaybeKey, Message, WorkerId,
};

use super::icadd_operator::TargetedMessage;
use super::{DistData, DistKey, DistTimestamp};

#[derive(Serialize, Deserialize)]
enum ExchangedMessage<K, V, T> {
    Barrier,
    Shutdown,
    Data(DataMessage<K, V, T>),
}

/// Builds an operator that excahnges jetstream messages
pub(crate) fn upstream_exchanger<K: DistKey, V: DistData, T: DistTimestamp>(
    ctx: &BuildContext,
) -> impl Logic<K, V, T, K, V, T> {
    let mut local_barrier: Option<Barrier> = None;

    // We may need to buffer some messages to align barriers
    let mut buffered_messages: IndexMap<WorkerId, Option<VecDeque<DataMessage<K, V, T>>>> = {
        let worker_ids = ctx
            .communication
            .get_peers()
            .into_iter()
            .cloned()
            .map(|k| (k, None));
        IndexMap::from_iter(worker_ids)
    };
    // we can't remove a worker from buffered_messages while barriers are being aligned
    // so we need to queue up this one
    let mut queued_shutdowns: VecDeque<WorkerId> = VecDeque::new();

    move |input, output, ctx| {
        if local_barrier.is_none() {
            if let Some(msg) = input.recv() {
                match msg {
                    Message::AbsBarrier(x) => {
                        ctx.communication
                            .broadcast(ExchangedMessage::<K, V, T>::Barrier);
                        local_barrier.replace(x);
                        try_clear_barrier(&mut buffered_messages, &mut local_barrier, output);
                        while let Some(w) = queued_shutdowns.pop_front() {
                            buffered_messages.swap_remove(&w);
                        }
                    }
                    Message::ShutdownMarker(x) => {
                        ctx.communication
                            .broadcast(ExchangedMessage::<K, V, T>::Shutdown);
                        output.send(Message::ShutdownMarker(x))
                    }
                    x => output.send(x),
                };
            }
        }

        for network_msg in ctx.communication.recv_all::<ExchangedMessage<K, V, T>>() {
            match network_msg.data {
                ExchangedMessage::Barrier => {
                    buffered_messages.insert(network_msg.sender_worker, Some(VecDeque::new()));
                    try_clear_barrier(&mut buffered_messages, &mut local_barrier, output);
                    while let Some(w) = queued_shutdowns.pop_front() {
                        buffered_messages.swap_remove(&w);
                    }
                }
                ExchangedMessage::Shutdown => {
                    if local_barrier.is_some() || buffered_messages.values().any(|x| x.is_some()) {
                        // snapshot in progress
                        queued_shutdowns.push_back(network_msg.sender_worker);
                    } else {
                        buffered_messages.swap_remove(&network_msg.sender_worker);
                    }
                }
                ExchangedMessage::Data(d) => {
                    if let Some(b) = buffered_messages
                        .get_mut(&network_msg.sender_worker)
                        .unwrap()
                    {
                        b.push_back(d)
                    } else {
                        output.send(Message::Data(d))
                    }
                }
            }
        }
    }
}

fn try_clear_barrier<K: Clone, V: Clone, T: Clone>(
    received_barriers: &mut IndexMap<WorkerId, Option<VecDeque<DataMessage<K, V, T>>>>,
    local_barrier: &mut Option<Barrier>,
    output: &mut Sender<K, V, T>,
) {
    if received_barriers.values().all(|x| x.is_some()) {
        if let Some(b) = local_barrier.take() {
            output.send(Message::AbsBarrier(b));
            // remove all buffered datamessages
            for d in received_barriers
                .values_mut()
                .filter_map(|x| x.take())
                .flat_map(|x| x.into_iter())
            {
                output.send(Message::Data(d))
            }
        }
    }
}


/// Builds an operator that excahnges jetstream messages
pub(crate) fn downstream_exchanger<K: DistKey, V: DistData, T: DistTimestamp>(
    upstream_offset: usize,
    ctx: &BuildContext,
) -> impl Logic<K, TargetedMessage<V>, T, K, V, T> {
    let upstream = ctx.operator_id - upstream_offset;

    move |input, output, op_ctx| {
        if let Some(msg) = input.recv() {
            match msg {
                Message::Data(d) => {
                    let target = d.value.target;
                    let vmsg = d.value.inner;
                    if target.map_or(true, |x| x == op_ctx.worker_id) {
                        output.send(Message::Data(DataMessage::new(d.key, vmsg.inner, d.timestamp)))
                    } else {
                        let exmsg = ExchangedMessage::Data(DataMessage::new(d.key, vmsg, d.timestamp));
                        op_ctx.communication.send_other_operator(&target.unwrap(), &upstream, exmsg).unwrap();
                    }

                }
                Message::AbsBarrier(b) => output.send(Message::AbsBarrier(b)),
                Message::Epoch(e) => output.send(Message::Epoch(e)),
                Message::Rescale(r) => output.send(Message::Rescale(r)),
                Message::ShutdownMarker(s) => output.send( Message::ShutdownMarker(s)),
                Message::Interrogate(i) => output.send(Message::Interrogate(i)),
                Message::Collect(c) => output.send(Message::Collect(c)),
                Message::Acquire(a) => output.send(Message::Acquire(a)),
                Message::DropKey(d) => output.send(Message::DropKey(d)),
            }
        }
    }
}


#[cfg(test)]
mod test {
    use std::{rc::Rc, sync::Mutex, time::Duration};

    use super::*;
    use crate::{
        keyed::distributed::{Acquire, Collect, Interrogate},
        snapshot::{Barrier, NoPersistence},
        test::OperatorTester,
        time::MaybeTime,
        DataMessage, MaybeData, MaybeKey, Message, OperatorId, RescaleMessage, ShutdownMarker,
        WorkerId,
    };
    use indexmap::IndexSet;
    use postbox::{NetworkMessage, RecvIterator};
    use serde::{de::DeserializeOwned, Serialize};

    fn loop_till_recv_from_remote<KI, VI, TI, KO, VO, TO, U>(
        tester: &mut OperatorTester<KI, VI, TI, KO, VO, TO>,
    ) -> NetworkMessage<WorkerId, OperatorId, U>
    where
        KI: MaybeKey,
        VI: MaybeData,
        TI: MaybeTime,
        KO: MaybeKey,
        VO: MaybeData,
        TO: MaybeTime,
        U: Serialize + DeserializeOwned,
    {
        loop {
            tester.step();
            if let Some(x) = tester.receive_on_remote::<U>().next() {
                return x;
            }
        }
    }
    fn loop_till_recv_from_local<KI, VI, TI, KO, VO, TO>(
        tester: &mut OperatorTester<KI, VI, TI, KO, VO, TO>,
    ) -> Message<KO, VO, TO>
    where
        KI: MaybeKey,
        VI: MaybeData,
        TI: MaybeTime,
        KO: MaybeKey,
        VO: MaybeData,
        TO: MaybeTime,
    {
        loop {
            tester.step();
            if let Some(x) = tester.receive_on_local() {
                return x;
            }
        }
    }
    /// A barrier coming in from local upstream should be broadcasted
    #[test]
    fn broadcast_barrier() {
        let mut tester = OperatorTester::new_built_by(upstream_exchanger::<i32, String, u64>);
        tester.send_from_local(Message::AbsBarrier(Barrier::new(Box::new(
            NoPersistence::default(),
        ))));

        let remote_result: NetworkMessage<
            WorkerId,
            OperatorId,
            ExchangedMessage<i32, String, u64>,
        > = loop_till_recv_from_remote(&mut tester);
        assert!(matches!(remote_result.data, ExchangedMessage::Barrier))
    }

    /// A shutdown marker coming in from local upstream should be broadcasted and sent downstream
    #[test]
    fn broadcast_shutdown() {
        let mut tester = OperatorTester::new_built_by(upstream_exchanger::<i32, String, u64>);
        tester.send_from_local(Message::ShutdownMarker(ShutdownMarker::default()));
        tester.step();
        let local_result = tester.receive_on_local().unwrap();
        assert!(matches!(local_result, Message::ShutdownMarker(_)));

        let remote_result: NetworkMessage<
            WorkerId,
            OperatorId,
            ExchangedMessage<i32, String, u64>,
        > = loop_till_recv_from_remote(&mut tester);
        assert!(matches!(remote_result.data, ExchangedMessage::Shutdown))
    }

    /// A barrier received from a local upstream should not trigger any output, when there is no state on the remote barrier
    #[test]
    fn align_barrier_from_local_none() {
        let mut tester = OperatorTester::new_built_by(upstream_exchanger::<i32, String, u64>);
        tester.send_from_local(Message::AbsBarrier(Barrier::new(Box::new(
            NoPersistence::default(),
        ))));

        tester.step();
        assert!(tester.receive_on_local().is_none());
    }

    /// A barrier received from a remote should not trigger any output, when there is no state on the local barrier
    #[test]
    fn align_barrier_from_remote_none() {
        let mut tester = OperatorTester::new_built_by(upstream_exchanger::<i32, String, u64>);
        tester.send_from_remote(ExchangedMessage::<i32, String, u64>::Barrier);
        // HACK: Wait some time to so the message is actually delivered
        std::thread::sleep(Duration::from_secs(3));
        tester.step();
        assert!(tester.receive_on_local().is_none());
    }

    /// A barrier received from a local upstream should trigger a barrier output when there is state
    /// for the remote
    #[test]
    fn align_barrier_from_local() {
        let mut tester = OperatorTester::new_built_by(upstream_exchanger::<i32, String, u64>);
        tester.send_from_remote(ExchangedMessage::<i32, String, u64>::Barrier);
        // TODO: find a more elegant way to do this
        std::thread::sleep(Duration::from_secs(3));
        tester.send_from_local(Message::AbsBarrier(Barrier::new(Box::new(
            NoPersistence::default(),
        ))));

        tester.step();
        let local_result = tester.receive_on_local().unwrap();
        assert!(
            matches!(local_result, Message::AbsBarrier(_)),
            "{local_result:?}"
        );
    }

    /// A barrier received from a local upstream should trigger a barrier output when there is state
    /// for the remote but the next barrier should need to be aligned again
    #[test]
    fn align_barrier_from_local_twice() {
        let mut tester = OperatorTester::new_built_by(upstream_exchanger::<i32, String, u64>);
        tester.send_from_remote(ExchangedMessage::<i32, String, u64>::Barrier);
        // TODO: find a more elegant way to do this
        std::thread::sleep(Duration::from_secs(3));
        tester.send_from_local(Message::AbsBarrier(Barrier::new(Box::new(
            NoPersistence::default(),
        ))));

        tester.step();
        let local_result = tester.receive_on_local().unwrap();
        assert!(
            matches!(local_result, Message::AbsBarrier(_)),
            "{local_result:?}"
        );
        tester.send_from_local(Message::AbsBarrier(Barrier::new(Box::new(
            NoPersistence::default(),
        ))));
        tester.step();
        assert!(tester.receive_on_local().is_none());
    }

    /// A barrier received from a remote should trigger a barrier output when there is state
    /// for the local barrier
    #[test]
    fn align_barrier_from_remote() {
        let mut tester = OperatorTester::new_built_by(upstream_exchanger::<i32, String, u64>);
        tester.send_from_local(Message::AbsBarrier(Barrier::new(Box::new(
            NoPersistence::default(),
        ))));
        tester.send_from_remote(ExchangedMessage::<i32, String, u64>::Barrier);
        // TODO: find a more elegant way to do this
        std::thread::sleep(Duration::from_secs(3));
        tester.step();
        let local_result = tester.receive_on_local().unwrap();
        assert!(
            matches!(local_result, Message::AbsBarrier(_)),
            "{local_result:?}"
        );
    }

    /// If we receive a shutdownmarker from a remote and that remote was previously holding back the
    /// advancement of the barrier, the barrier should advance after the remote has shut down
    #[test]
    fn advance_barrier_after_remote_shutdown() {
        let mut tester = OperatorTester::new_built_by(upstream_exchanger::<i32, String, u64>);
        tester.send_from_local(Message::AbsBarrier(Barrier::new(Box::new(
            NoPersistence::default(),
        ))));
        tester.send_from_remote(ExchangedMessage::<i32, String, u64>::Shutdown);
        let advanced = loop_till_recv_from_local(&mut tester);

        assert!(matches!(advanced, Message::AbsBarrier(_)));
    }

    /// These message types should simply pass the operator
    #[test]
    fn pass_through_operator() {
        let messages = vec![
            Message::Data(DataMessage::new(1, "Hello".to_string(), 2)),
            Message::Epoch(42),
            Message::Rescale(RescaleMessage::ScaleAddWorker(IndexSet::new())),
            Message::Rescale(RescaleMessage::ScaleRemoveWorker(IndexSet::new())),
            Message::Interrogate(Interrogate::new(
                IndexSet::new(),
                Rc::new(|_| false),
            )),
            Message::Collect(Collect::new(1)),
            Message::Acquire(Acquire::new(1, Rc::new(Mutex::new(IndexMap::new())))),
            Message::DropKey(1),
        ];
        let mut tester = OperatorTester::new_built_by(upstream_exchanger::<i32, String, u64>);
        for m in messages.into_iter() {
            tester.send_from_local(m.clone());
            tester.step();
            assert!(tester.receive_on_local().is_some());
        }
    }

    /// It must not forward any data before the barriers are aligned
    #[test]
    fn no_barrier_overtaking_remote_barrier() {
        let mut tester = OperatorTester::new_built_by(upstream_exchanger::<i32, String, u64>);
        tester.send_from_remote(ExchangedMessage::<i32, String, u64>::Barrier);
        tester.send_from_remote(ExchangedMessage::<i32, String, u64>::Data(
            DataMessage::new(1, "Hi".into(), 10),
        ));
        // TODO: find a more elegant way to do this
        std::thread::sleep(Duration::from_secs(3));
        tester.step();
        assert!(tester.receive_on_local().is_none());
        tester.send_from_local(Message::AbsBarrier(Barrier::new(Box::new(
            NoPersistence::default(),
        ))));
        std::thread::sleep(Duration::from_secs(1));
        let barrier = loop_till_recv_from_local(&mut tester);
        let message = loop_till_recv_from_local(&mut tester);
        assert!(matches!(barrier, Message::AbsBarrier(_)));
        assert!(matches!(message, Message::Data(_)));
    }

    /// It must not forward any data before the barriers are aligned
    #[test]
    fn no_barrier_overtaking_local_barrier() {
        let mut tester = OperatorTester::new_built_by(upstream_exchanger::<i32, String, u64>);
        tester.send_from_local(Message::AbsBarrier(Barrier::new(Box::new(
            NoPersistence::default(),
        ))));
        tester.send_from_local(Message::Data(DataMessage::new(1, "Hi".into(), 10)));
        tester.step();
        assert!(tester.receive_on_local().is_none());
        tester.send_from_remote(ExchangedMessage::<i32, String, u64>::Barrier);
        std::thread::sleep(Duration::from_secs(1));
        let barrier = loop_till_recv_from_local(&mut tester);
        let message = loop_till_recv_from_local(&mut tester);
        assert!(matches!(barrier, Message::AbsBarrier(_)));
        assert!(matches!(message, Message::Data(_)));
    }
}
