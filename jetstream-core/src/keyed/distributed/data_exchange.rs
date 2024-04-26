use std::collections::VecDeque;
use std::iter;

use indexmap::IndexMap;
use postbox::{broadcast, Client};
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
use super::versioner::VersionedMessage;
use super::{DistData, DistKey, DistTimestamp};

#[derive(Serialize, Deserialize, Clone)]
enum ExchangedMessage<K, V, T> {
    Barrier,
    Shutdown,
    Data(DataMessage<K, V, T>),
}

/// Builds an operator that excahnges jetstream messages
pub(crate) fn upstream_exchanger<K: DistKey, V: DistData, T: DistTimestamp>(
    ctx: &mut BuildContext,
) -> impl Logic<K, VersionedMessage<V>, T, K, VersionedMessage<V>, T> {
    let mut local_barrier: Option<Barrier> = None;
    // tuple of client and is_barred
    let mut clients: IndexMap<
        WorkerId,
        (Client<ExchangedMessage<K, VersionedMessage<V>, T>>, bool),
    > = ctx
        .get_worker_ids()
        .filter_map(|i| {
            if i != ctx.worker_id {
                Some((
                    i,
                    (ctx.create_communication_client(i, ctx.operator_id), false),
                ))
            } else {
                None
            }
        })
        .collect();

    move |input, output, ctx| {
        if local_barrier.is_none() {
            if let Some(msg) = input.recv() {
                match msg {
                    Message::AbsBarrier(x) => {
                        broadcast(clients.values().map(|x| &x.0), ExchangedMessage::Barrier)
                            .unwrap();
                        local_barrier.replace(x);
                    }
                    Message::ShutdownMarker(x) => {
                        broadcast(clients.values().map(|x| &x.0), ExchangedMessage::Shutdown)
                            .unwrap();
                        output.send(Message::ShutdownMarker(x))
                    }
                    x => output.send(x),
                };
            }
        }

        for (client, is_barred) in clients.values_mut() {
            if *is_barred {
                continue;
            }
            for network_msg in client.recv_all().map(|x| x.unwrap()) {
                match network_msg {
                    ExchangedMessage::Barrier => {
                        *is_barred = true;
                        break;
                    }
                    ExchangedMessage::Shutdown => {
                        todo!()
                    }
                    ExchangedMessage::Data(d) => output.send(Message::Data(d)),
                }
            }
        }

        // try releasing the barrier
        if local_barrier.is_some() && clients.values().all(|x| x.1) {
            output.send(Message::AbsBarrier(local_barrier.take().unwrap()));
            for c in clients.values_mut() {
                c.1 = false;
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
    ctx: &mut BuildContext,
) -> impl Logic<K, TargetedMessage<V>, T, K, V, T> {
    let upstream = ctx.operator_id - upstream_offset;
    let mut clients: IndexMap<WorkerId, Client<ExchangedMessage<K, VersionedMessage<V>, T>>> = ctx
        .get_worker_ids()
        .filter_map(|i| {
            if i != ctx.worker_id {
                Some((
                    i,
                    ctx.create_communication_client(i, ctx.operator_id),
                ))
            } else {
                None
            }
        })
        .collect();

    move |input, output, op_ctx| {
        if let Some(msg) = input.recv() {
            match msg {
                Message::Data(d) => {
                    let target = d.value.target;
                    let vmsg = d.value.inner;

                    let target = target.unwrap_or(op_ctx.worker_id);
                    if target == op_ctx.worker_id {
                        output.send(Message::Data(DataMessage::new(
                            d.key,
                            vmsg.inner,
                            d.timestamp,
                        )))
                    } else {
                        let exmsg =
                            ExchangedMessage::Data(DataMessage::new(d.key, vmsg, d.timestamp));
                        clients
                            .get(&target)
                            .expect("Partitioner target out of range")
                            .send(exmsg)
                            .unwrap()
                    }
                }
                Message::AbsBarrier(b) => output.send(Message::AbsBarrier(b)),
                Message::Epoch(e) => output.send(Message::Epoch(e)),
                Message::Rescale(r) => output.send(Message::Rescale(r)),
                Message::ShutdownMarker(s) => output.send(Message::ShutdownMarker(s)),
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
    use serde::{de::DeserializeOwned, Serialize};

    fn loop_till_recv_from_remote<KI, VI, TI, KO, VO, TO, R>(
        tester: &mut OperatorTester<KI, VI, TI, KO, VO, TO, R>,
    ) -> R
    where
        KI: MaybeKey,
        VI: MaybeData,
        TI: MaybeTime,
        KO: MaybeKey,
        VO: MaybeData,
        TO: MaybeTime,
        R: postbox::Data,
    {
        loop {
            tester.step();
            if let Some(x) = tester.receive_on_remote() {
                return x;
            }
        }
    }
    fn loop_till_recv_from_local<KI, VI, TI, KO, VO, TO, R>(
        tester: &mut OperatorTester<KI, VI, TI, KO, VO, TO, R>,
    ) -> Message<KO, VO, TO>
    where
        KI: MaybeKey,
        VI: MaybeData,
        TI: MaybeTime,
        KO: MaybeKey,
        VO: MaybeData,
        TO: MaybeTime,
        R: postbox::Data,
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

        let remote_result: ExchangedMessage<i32, String, u64> =
            loop_till_recv_from_remote(&mut tester);
        assert!(matches!(remote_result, ExchangedMessage::Barrier))
    }

    /// A shutdown marker coming in from local upstream should be broadcasted and sent downstream
    #[test]
    fn broadcast_shutdown() {
        let mut tester = OperatorTester::new_built_by(upstream_exchanger::<i32, String, u64>);
        tester.send_from_local(Message::ShutdownMarker(ShutdownMarker::default()));
        tester.step();
        let local_result = tester.receive_on_local().unwrap();
        assert!(matches!(local_result, Message::ShutdownMarker(_)));

        let remote_result: ExchangedMessage<i32, String, u64> =
            loop_till_recv_from_remote(&mut tester);
        assert!(matches!(remote_result, ExchangedMessage::Shutdown))
    }

    /// A barrier received from a local upstream should not trigger any output, when there is no state on the remote barrier
    #[test]
    fn align_barrier_from_local_none() {
        let mut tester: OperatorTester<
            i32,
            VersionedMessage<String>,
            u64,
            i32,
            VersionedMessage<String>,
            u64,
            (),
        > = OperatorTester::new_built_by(upstream_exchanger::<i32, String, u64>);
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
            Message::Data(DataMessage::new(
                1,
                VersionedMessage::new("Hello".to_string(), 0, 0),
                2,
            )),
            Message::Epoch(42),
            Message::Rescale(RescaleMessage::ScaleAddWorker(IndexSet::new())),
            Message::Rescale(RescaleMessage::ScaleRemoveWorker(IndexSet::new())),
            Message::Interrogate(Interrogate::new(IndexSet::new(), Rc::new(|_| false))),
            Message::Collect(Collect::new(1)),
            Message::Acquire(Acquire::new(1, Rc::new(Mutex::new(IndexMap::new())))),
            Message::DropKey(1),
        ];
        let mut tester: OperatorTester<
            i32,
            VersionedMessage<String>,
            u64,
            i32,
            VersionedMessage<String>,
            u64,
            (),
        > = OperatorTester::new_built_by(upstream_exchanger::<i32, String, u64>);
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
        tester.send_from_remote(ExchangedMessage::<i32, VersionedMessage<String>, u64>::Barrier);
        tester.send_from_remote(ExchangedMessage::<i32, VersionedMessage<String>, u64>::Data(
            DataMessage::new(1, VersionedMessage::new("Hi".into(), 0, 0), 10),
        ));

        // TODO: find a more elegant way to do this
        std::thread::sleep(Duration::from_secs(1));
        tester.step();

        let msg = tester.receive_on_local();
        assert!(msg.is_none(), "{:?}", msg);
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
        tester.send_from_local(Message::Data(DataMessage::new(
            1,
            VersionedMessage::new("Hi".into(), 0, 0),
            10,
        )));
        tester.step();
        assert!(tester.receive_on_local().is_none());
        tester.send_from_remote(ExchangedMessage::<i32, VersionedMessage<String>, u64>::Barrier);
        std::thread::sleep(Duration::from_secs(1));
        let barrier = loop_till_recv_from_local(&mut tester);
        let message = loop_till_recv_from_local(&mut tester);
        assert!(matches!(barrier, Message::AbsBarrier(_)));
        assert!(matches!(message, Message::Data(_)));
    }
}
