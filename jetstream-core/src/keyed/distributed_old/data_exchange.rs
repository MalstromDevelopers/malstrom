//! These operators do the actual exchange of data messages between workers
use indexmap::IndexMap;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::keyed::types::{DistData, DistKey, DistTimestamp};
use crate::runtime::communication::{broadcast, Distributable};
use crate::runtime::CommunicationClient;
use crate::snapshot::Barrier;
use crate::stream::{BuildContext, OperatorContext};
use crate::stream::Logic;
use crate::types::{DataMessage, Message, ShutdownMarker, WorkerId};

use super::icadd_operator::TargetedMessage;
use super::versioner::VersionedMessage;

enum ExchangedMessage<K, V, T> {
    Barrier,
    Shutdown,
    Data(DataMessage<K, V, T>),
}
#[derive(Serialize, Deserialize, Clone)]
enum SymmetricMessage {
    Barrier,
    Shutdown,
}

/// Holds the communication clients we need
struct ClientContainer<K, V, T> {
    // client to the other upstream_exchangers
    symmetric_client: CommunicationClient<SymmetricMessage>,
    // client to the other worker's downstreams, which send us data
    downstream_client: CommunicationClient<DataMessage<K, VersionedMessage<V>, T>>,
    // whether the symmetric client is currently barred
    is_barred: bool
}
impl<K, V, T> ClientContainer<K, V, T> where K: Distributable, V: Distributable, T: Distributable {
    fn new(wid: WorkerId, downstream_offset: usize, ctx: &mut OperatorContext) -> Self {
        let symmetric_client = ctx.create_communication_client(wid, ctx.operator_id);
        let downstream_client = ctx.create_communication_client(wid, ctx.operator_id + downstream_offset);
        let is_barred = false;
        Self { symmetric_client, downstream_client, is_barred }
    }
}

/// Builds an operator that exchanges jetstream messages
/// This one sits in front (upstream) of our distributor
pub(crate) fn upstream_exchanger<K: DistKey, V: DistData, T: DistTimestamp>(
    downstream_offset: usize,
    ctx: &mut BuildContext,
) -> impl Logic<K, VersionedMessage<V>, T, K, VersionedMessage<V>, T> {
    let mut local_barrier: Option<Barrier> = None;
    let mut local_shutdown: Option<ShutdownMarker> = None;

    let other_workers = ctx.get_worker_ids().filter(|x| *x != ctx.worker_id);
    let clients: IndexMap<WorkerId, ClientContainer<K, V, T>> = other_workers.map(|wid| (wid, ClientContainer::new(wid, downstream_offset, ctx))).collect();
    
    move |input, output, ctx| {
        if local_barrier.is_none() {
            if let Some(msg) = input.recv() {
                match msg {
                    Message::AbsBarrier(x) => {
                        broadcast(
                            clients.values().map(|x| &x.symmetric_client),
                            SymmetricMessage::Barrier,
                        );
                        local_barrier.replace(x);
                    }
                    Message::ShutdownMarker(x) => {
                        broadcast(
                            clients.values().map(|x| &x.symmetric_client),
                            SymmetricMessage::Shutdown,
                        );
                        // shutdownmarker needs to get aligned before we can send it downstream
                        local_shutdown = Some(x);
                    }
                    x => output.send(x),
                };
            }
        }

        clients.retain(|worker_id, clients| {
            if clients.is_barred {
                return true;
            }
            match clients.downstream_client.recv() {
                Some(x) => output.send(Message::Data(d))
            }

            // only read one message from the buffer, to make it "fair" 
            // with our own upstream
                if let Some(msg) = .recv() {

                    match msg {
                        ExchangedMessage::Shutdown => {remote_barrier_received.swap_remove(worker_id); false},
                        ExchangedMessage::Barrier => {*remote_barrier_received.get_mut(worker_id).unwrap() = true; true}
                        ExchangedMessage::Data(_) => unreachable!("Symmetric clients only exchange markers"),
                }
                } else {
                    true
                }
        });

        downstream_clients.retain(|worker_id, client| {
            if *remote_barrier_received.get(worker_id).expect("Maps are aligned") {
                // client is barred
                return true;
            }
            // only read one message from the buffer, to make it "fair" 
            // with our own upstream
                match client.recv() {
                    Some(ExchangedMessage::Data(d)) => {output.send(Message::Data(d)); true},
                    Some(ExchangedMessage::Shutdown) => false,
                    Some(ExchangedMessage::Barrier) => unreachable!("Barriers can not travel upstream"),
                    None => true
            }
        });
     
        // try releasing the barrier
        if local_barrier.is_some() && remote_barrier_received.values().all(|x| *x) {
            output.send(Message::AbsBarrier(local_barrier.take().unwrap()));
            for (_k, v) in remote_barrier_received.iter_mut() {
                *v = false;
            }
        }
        // only allow shutdown if we have cleanly dropped all connections
        if local_shutdown.is_some() && symmetric_clients.is_empty() {
            let oid = ctx.operator_id;
            println!("Emitting shutdown on op {oid}");
            output.send(Message::ShutdownMarker(local_shutdown.take().unwrap()));
        }
    }
}

/// Builds an operator that excahnges jetstream messages
/// This one sits after (downstream) of the distributor
pub(crate) fn downstream_exchanger<K: DistKey, V: DistData, T: DistTimestamp>(
    upstream_offset: usize,
    ctx: &mut BuildContext,
) -> impl Logic<K, TargetedMessage<V>, T, K, V, T> {
    let upstream = ctx.operator_id - upstream_offset;

    let clients: IndexMap<
        WorkerId,
        CommunicationClient<ExchangedMessage<K, VersionedMessage<V>, T>>,
    > = ctx
        .get_worker_ids()
        .filter_map(|i| {
            (i != ctx.worker_id).then_some((i, ctx.create_communication_client(i, upstream)))
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
                    }
                }
                Message::AbsBarrier(b) => output.send(Message::AbsBarrier(b)),
                Message::Epoch(e) => output.send(Message::Epoch(e)),
                Message::Rescale(r) => output.send(Message::Rescale(r)),
                Message::ShutdownMarker(s) => {
                    let wid = op_ctx.worker_id;
                    println!("Shutting down downstream of {wid}");
                    // broadcast(clients.values(), ExchangedMessage::Shutdown).unwrap();
                    output.send(Message::ShutdownMarker(s))
                }
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
    use std::{rc::Rc, sync::Mutex};

    use indexmap::IndexSet;

    use super::*;
    use crate::{
        keyed::distributed::{Acquire, Collect, Interrogate},
        snapshot::NoPersistence,
        testing::{OperatorTester, SentMessage},
        types::*,
    };

    /// A barrier coming in from local upstream should be broadcasted
    #[test]
    fn broadcast_barrier() {
        let mut tester = OperatorTester::built_by(
            |ctx| upstream_exchanger::<i32, String, u64>(2, ctx),
            0,
            1,
            0..2,
        );
        tester.send_local(Message::AbsBarrier(Barrier::new(Box::new(
            NoPersistence::default(),
        ))));
        tester.step();
        let remote_result: SentMessage<ExchangedMessage<i32, String, u64>> =
            tester.remote().recv_from_operator().unwrap();
        assert!(matches!(
            remote_result,
            SentMessage {
                worker_id: 1,
                operator_id: 1,
                msg: ExchangedMessage::Barrier
            }
        ))
    }

    /// A shutdown marker coming in from local upstream should be broadcasted and sent downstream
    #[test]
    fn broadcast_shutdown() {
        let mut tester = OperatorTester::built_by(
            |ctx| upstream_exchanger::<i32, String, u64>(2, ctx),
            0,
            1,
            0..1,
        );
        tester.send_local(Message::ShutdownMarker(ShutdownMarker::default()));
        tester.step();
        let local_result = tester.recv_local().unwrap();
        assert!(matches!(local_result, Message::ShutdownMarker(_)));

        let remote_result: SentMessage<ExchangedMessage<i32, String, u64>> =
            tester.remote().recv_from_operator().unwrap();
        assert!(matches!(
            remote_result,
            SentMessage {
                worker_id: 0,
                operator_id: 1,
                msg: ExchangedMessage::Shutdown
            }
        ))
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
        > = OperatorTester::built_by(
            |ctx| upstream_exchanger::<i32, String, u64>(2, ctx),
            0,
            1,
            0..2,
        );
        tester.send_local(Message::AbsBarrier(Barrier::new(Box::new(
            NoPersistence::default(),
        ))));
        tester.step();

        assert!(tester.recv_local().is_none());
    }

    /// A barrier received from a remote should not trigger any output, when there is no state on the local barrier
    #[test]
    fn align_barrier_from_remote_none() {
        let mut tester = OperatorTester::built_by(
            |ctx| upstream_exchanger::<i32, String, u64>(2, ctx),
            0,
            1,
            0..2,
        );
        tester
            .remote()
            .send_to_operator(ExchangedMessage::<i32, String, u64>::Barrier, 1, 1);
        tester.step();
        assert!(tester.recv_local().is_none());
    }

    /// A barrier received from a local upstream should trigger a barrier output when there is state
    /// for the remote
    #[test]
    fn align_barrier_from_local() {
        let mut tester = OperatorTester::built_by(
            |ctx| upstream_exchanger::<i32, String, u64>(2, ctx),
            0,
            1,
            0..2,
        );
        tester
            .remote()
            .send_to_operator(ExchangedMessage::<i32, String, u64>::Barrier, 1, 1);
        tester.send_local(Message::AbsBarrier(Barrier::new(Box::new(
            NoPersistence::default(),
        ))));

        tester.step();
        let local_result = tester.recv_local().unwrap();
        assert!(
            matches!(local_result, Message::AbsBarrier(_)),
            "{local_result:?}"
        );
    }

    /// A barrier received from a local upstream should trigger a barrier output when there is state
    /// for the remote but the next barrier should need to be aligned again
    #[test]
    fn align_barrier_from_local_twice() {
        let mut tester = OperatorTester::built_by(
            |ctx| upstream_exchanger::<i32, String, u64>(2, ctx),
            0,
            1,
            0..2,
        );
        tester
            .remote()
            .send_to_operator(ExchangedMessage::<i32, String, u64>::Barrier, 1, 1);
        tester.send_local(Message::AbsBarrier(Barrier::new(Box::new(
            NoPersistence::default(),
        ))));

        tester.step();
        let local_result = tester.recv_local().unwrap();
        assert!(
            matches!(local_result, Message::AbsBarrier(_)),
            "{local_result:?}"
        );
        tester.send_local(Message::AbsBarrier(Barrier::new(Box::new(
            NoPersistence::default(),
        ))));
        tester.step();
        assert!(tester.recv_local().is_none());
    }

    /// A barrier received from a remote should trigger a barrier output when there is state
    /// for the local barrier
    #[test]
    fn align_barrier_from_remote() {
        let mut tester = OperatorTester::built_by(
            |ctx| upstream_exchanger::<i32, String, u64>(2, ctx),
            0,
            1,
            0..2,
        );
        tester.send_local(Message::AbsBarrier(Barrier::new(Box::new(
            NoPersistence::default(),
        ))));
        tester
            .remote()
            .send_to_operator(ExchangedMessage::<i32, String, u64>::Barrier, 1, 1);
        tester.step();
        let local_result = tester.recv_local().unwrap();
        assert!(
            matches!(local_result, Message::AbsBarrier(_)),
            "{local_result:?}"
        );
    }

    /// If we receive a shutdownmarker from a remote and that remote was previously holding back the
    /// advancement of the barrier, the barrier should advance after the remote has shut down
    #[test]
    fn advance_barrier_after_remote_shutdown() {
        let mut tester = OperatorTester::built_by(
            |ctx| upstream_exchanger::<i32, String, u64>(2, ctx),
            0,
            1,
            0..2,
        );
        tester.send_local(Message::AbsBarrier(Barrier::new(Box::new(
            NoPersistence::default(),
        ))));
        tester
            .remote()
            .send_to_operator(ExchangedMessage::<i32, String, u64>::Shutdown, 1, 1);
        tester.step();
        let advanced = tester.recv_local().unwrap();

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
            Message::Interrogate(Interrogate::new(Rc::new(|_| false))),
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
        > = OperatorTester::built_by(
            |ctx| upstream_exchanger::<i32, String, u64>(2, ctx),
            0,
            1,
            0..1,
        );
        for m in messages.into_iter() {
            tester.send_local(m.clone());
            tester.step();
            assert!(tester.recv_local().is_some());
        }
    }

    /// It must not forward any data before the barriers are aligned
    #[test]
    fn no_barrier_overtaking_remote_barrier() {
        let mut tester = OperatorTester::built_by(
            |ctx| upstream_exchanger::<i32, String, u64>(2, ctx),
            0,
            1,
            0..2,
        );
        // send a barrier to "block" the operator from forwarding data
        tester.remote().send_to_operator(
            ExchangedMessage::<i32, VersionedMessage<String>, u64>::Barrier,
            1,
            1,
        );
        tester.remote().send_to_operator(
            ExchangedMessage::<i32, VersionedMessage<String>, u64>::Data(DataMessage::new(
                1,
                VersionedMessage::new("Hi".into(), 0, 0),
                10,
            )),
            1,
            3,
        );

        tester.step();
        
        // this should be none since the operator will block until
        // it gets a barrier message from upstream too
        let msg = tester.recv_local();
        assert!(msg.is_none(), "{msg:?}");
        tester.send_local(Message::AbsBarrier(Barrier::new(Box::new(
            NoPersistence::default(),
        ))));
        tester.step();
        tester.step();

        let barrier = tester.recv_local().unwrap();
        let message = tester.recv_local().unwrap();
        assert!(matches!(barrier, Message::AbsBarrier(_)));
        assert!(matches!(message, Message::Data(_)));
    }

    /// It must not forward any data before the barriers are aligned
    #[test]
    fn no_barrier_overtaking_local_barrier() {
        let mut tester = OperatorTester::built_by(
            |ctx| upstream_exchanger::<i32, String, u64>(2, ctx),
            0,
            1,
            0..1,
        );
        tester.send_local(Message::AbsBarrier(Barrier::new(Box::new(
            NoPersistence::default(),
        ))));
        tester.send_local(Message::Data(DataMessage::new(
            1,
            VersionedMessage::new("Hi".into(), 0, 0),
            10,
        )));
        tester.step();
        assert!(tester.recv_local().is_none());

        tester.remote().send_to_operator(
            ExchangedMessage::<i32, VersionedMessage<String>, u64>::Barrier,
            1,
            1,
        );
        tester.step();
        let barrier = tester.recv_local().unwrap();
        let message = tester.recv_local().unwrap();
        assert!(matches!(barrier, Message::AbsBarrier(_)));
        assert!(matches!(message, Message::Data(_)));
    }
}
