use indexmap::IndexMap;
use itertools::Itertools;

mod message_router;
pub mod types;

use std::iter::once;

use message_router::{MessageRouter, NormalRouter};
pub use types::*;

use crate::{
    channels::operator_io::{Input, Output},
    runtime::BiCommunicationClient,
    snapshot::Barrier,
    stream::{BuildContext, OperatorContext},
    types::{DataMessage, MaybeTime, Message, RescaleMessage, SuspendMarker, WorkerId},
};

use crate::runtime::communication::broadcast;

type Remotes<K, V, T> = IndexMap<
    WorkerId,
    (
        BiCommunicationClient<NetworkMessage<K, V, T>>,
        RemoteState<T>,
    ),
>;

pub(super) struct Distributor<K, V, T> {
    router: Container<MessageRouter<K, V, T>>,
    remotes: Remotes<K, V, T>,
    partitioner: WorkerPartitioner<K>,
    local_barrier: Option<Barrier>,
    local_shutdown: Option<SuspendMarker>,
    local_frontier: Option<T>,
}

impl<K, V, T> Distributor<K, V, T>
where
    K: DistKey,
    V: DistData,
    T: DistTimestamp,
{
    pub(super) fn new(paritioner: WorkerPartitioner<K>, ctx: &mut BuildContext) -> Self {
        let snapshot: Option<(NormalRouter, IndexMap<WorkerId, RemoteState<T>>, Option<T>)> =
            ctx.load_state();
        let other_workers = ctx
            .get_worker_ids()
            .iter()
            .map(|x| *x)
            .filter(|x| *x != ctx.worker_id)
            .collect_vec();

        let (state, remotes, frontier) = match snapshot {
            Some((router, remote_states, local_frontier)) => {
                // restoring from a differently sized snapshot is not supported
                if remote_states.len() != other_workers.len() {
                    // +1 to include this worker
                    panic_wrong_scale(ctx.get_worker_ids().len(), remote_states.len() + 1);
                }
                let remotes = create_remotes(&other_workers, ctx);
                (MessageRouter::Normal(router), remotes, local_frontier)
            }
            None => {
                let remotes = create_remotes(&other_workers, ctx);
                let state = MessageRouter::new(
                    ctx.get_worker_ids().iter().map(|x| *x).collect(),
                    Version::default(),
                );
                (state, remotes, None)
            }
        };

        Self {
            router: Container::new(state),
            remotes,
            partitioner: paritioner,
            local_barrier: None,
            local_shutdown: None,
            local_frontier: frontier,
        }
    }

    /// Schedule this as an operator in the dataflow
    pub(super) fn run(
        &mut self,
        input: &mut Input<K, V, T>,
        output: &mut Output<K, V, T>,
        ctx: &mut OperatorContext,
    ) {
        // HACK we collect messages into the vec because
        // we can't hold onto a &self when invoking the handlers
        let remote_message: Vec<(WorkerId, NetworkMessage<K, V, T>)> = self
            .remotes
            .iter()
            .filter(|(_wid, (_client, state))| !state.is_barred && !state.sent_suspend)
            .filter_map(|(wid, (client, _state))| client.recv().map(|msg| (*wid, msg)))
            .collect();
        for (wid, msg) in remote_message.into_iter() {
            match msg {
                NetworkMessage::Data(data_message) => {
                    self.handle_remote_data_message(data_message, &wid, output, ctx)
                }
                NetworkMessage::Epoch(epoch) => {
                    self.remotes.get_mut(&wid).unwrap().1.frontier = Some(epoch.clone());
                    self.handle_epoch(output)
                }
                NetworkMessage::BarrierMarker => {
                    self.remotes.get_mut(&wid).unwrap().1.is_barred = true
                }
                NetworkMessage::SuspendMarker => {
                    self.remotes.get_mut(&wid).unwrap().1.sent_suspend = true
                }
                NetworkMessage::Acquire(network_acquire) => {
                    output.send(Message::Acquire(network_acquire.into()))
                }
                NetworkMessage::Upgrade(version) => {
                    self.remotes.get_mut(&wid).unwrap().1.last_version = version
                }
            }
        }

        // not allowed to receive local if barrier is not resolved
        if self.local_barrier.is_none() {
            if let Some(msg) = input.recv() {
                match msg {
                    Message::Data(msg) => self.handle_local_data_message(msg, output, ctx),
                    Message::Epoch(epoch) => {
                        // TODO must not allow epochs to overtake messages while rescaling
                        broadcast(
                            self.remotes.values().map(|x| &x.0),
                            NetworkMessage::Epoch(epoch.clone()),
                        );
                        self.local_frontier = Some(epoch);
                        self.handle_epoch(output)
                    }
                    Message::AbsBarrier(barrier) => self.handle_local_barrier(barrier),
                    Message::Rescale(rescale) => self.handle_rescale_message(rescale, output, ctx),
                    Message::SuspendMarker(shutdown_marker) => {
                        self.local_shutdown = Some(shutdown_marker);
                        broadcast(
                            self.remotes.values().map(|x| &x.0),
                            NetworkMessage::SuspendMarker,
                        );
                    }

                    // these ones we can just ignore
                    Message::Interrogate(_) => (),
                    Message::Collect(_) => (),
                    Message::Acquire(_) => (),
                }
            }
        }

        // try to clear these
        // Now you might be tempted to do this in an event driven way
        // where we only call these functions if we get shutdown or barrier
        // messages, but that does not handle the case where the removal
        // of another worker allows them to be emitted
        self.try_emit_barrier(output);
        self.try_emit_shutdown(output);
        self.router
            .apply(|x| x.lifecycle(self.partitioner, output, &mut self.remotes));
    }

    /// Handle a data message we received from our local upstream
    fn handle_local_data_message(
        &mut self,
        message: DataMessage<K, V, T>,
        output: &mut Output<K, V, T>,
        ctx: &OperatorContext,
    ) {
        let routing = {
            self.router.route_message(
                message,
                None,
                self.partitioner,
                ctx.worker_id,
                ctx.worker_id,
            )
        };
        if let Some((msg, target)) = routing {
            self.send_data_message(msg, target, output, ctx);
        }
    }

    fn handle_remote_data_message(
        &mut self,
        message: NetworkDataMessage<K, V, T>,
        sent_by: &WorkerId,
        output: &mut Output<K, V, T>,
        ctx: &OperatorContext,
    ) {
        let routing = {
            self.router.route_message(
                message.content,
                Some(message.version),
                self.partitioner,
                ctx.worker_id,
                *sent_by,
            )
        };
        if let Some((msg, target)) = routing {
            self.send_data_message(msg, target, output, ctx);
        }
    }

    fn send_data_message(
        &self,
        message: DataMessage<K, V, T>,
        target: WorkerId,
        output: &mut Output<K, V, T>,

        ctx: &OperatorContext,
    ) {
        match target == ctx.worker_id {
            true => output.send(Message::Data(message)),
            false => {
                let client = &self
                    .remotes
                    .get(&target)
                    .expect("Message routing returns valid WorkerId")
                    .0;
                let wrapped_msg = NetworkDataMessage {
                    content: message,
                    version: self.router.get_version(),
                };
                client.send(NetworkMessage::Data(wrapped_msg));
            }
        }
    }

    /// Handle an epoch we received from our local upstrea
    fn handle_epoch(&self, output: &mut Output<K, V, T>) {
        let all_timestamps = self
            .remotes
            .values()
            .map(|x| &x.1.frontier)
            .chain(once(&self.local_frontier));
        let merged = merge_timestamps(all_timestamps);
        if let Some(to_emit) = merged {
            output.send(Message::Epoch(to_emit));
        }
    }

    /// Handle a barrier we receive
    fn handle_local_barrier(&mut self, barrier: Barrier) {
        self.local_barrier = Some(barrier);
        broadcast(
            self.remotes.values().map(|x| &x.0),
            NetworkMessage::BarrierMarker,
        );
    }

    fn handle_rescale_message(
        &mut self,
        message: RescaleMessage,
        output: &mut Output<K, V, T>,
        ctx: &mut OperatorContext,
    ) {
        // we can not remove clients of workers here because we need them during the rescale
        // process. They are removed in the final step of the "Finished" distributor

        for wid in message.get_new_workers() {
            if !self.remotes.contains_key(wid) && *wid != ctx.worker_id {
                let comm_client = ctx.create_communication_client(*wid);
                let remote_state = RemoteState::default();
                self.remotes.insert(*wid, (comm_client, remote_state));
            }
        }
        self.router
            .apply(|router| router.handle_rescale(message, self.partitioner, output))
    }

    /// Emits a barrier to the output only and only if
    /// - we have one from our local upstream
    /// - we have one from every connected client
    #[inline]
    fn try_emit_barrier(&mut self, output: &mut Output<K, V, T>) {
        if self.local_barrier.is_some()
            && self
                .remotes
                .values()
                .all(|x| x.1.is_barred || x.1.sent_suspend)
        {
            let msg = Message::AbsBarrier(self.local_barrier.take().unwrap());
            output.send(msg);

            for (_, remote_state) in self.remotes.iter_mut().map(|x| x.1) {
                remote_state.is_barred = false;
            }
        }
    }

    /// Emits a shutdown to the output only and only if
    /// - we have one from our local upstream
    /// - we have one from every connected client
    #[inline]
    fn try_emit_shutdown(&mut self, output: &mut Output<K, V, T>) {
        if self.local_shutdown.is_some() && self.remotes.values().all(|x| x.1.sent_suspend) {
            let msg = Message::SuspendMarker(self.local_shutdown.take().unwrap());
            output.send(msg);
        }
    }
}

fn create_remotes<K, V, T>(other_workers: &[WorkerId], ctx: &mut BuildContext) -> Remotes<K, V, T>
where
    K: DistKey,
    V: DistData,
    T: DistTimestamp,
{
    let remotes = other_workers
        .iter()
        .map(|worker_id| {
            (
                *worker_id,
                (
                    ctx.create_communication_client(*worker_id),
                    RemoteState::default(),
                ),
            )
        })
        .collect();
    remotes
}

/// Small reducer hack, as we can't use iter::reduce because of ownership
fn merge_timestamps<'a, T: MaybeTime>(
    mut timestamps: impl Iterator<Item = &'a Option<T>>,
) -> Option<T> {
    let mut merged = timestamps.next()?.clone();
    for x in timestamps {
        if let Some(y) = x {
            merged = merged.and_then(|a| a.try_merge(y));
        } else {
            return None;
        }
    }
    merged
}

/// Panic if we are starting at a scale different from the snapshot
fn panic_wrong_scale(build_scale: usize, snapshot_scale: usize) {
    panic!(
        "Attempted to build a Cluster of scale '{build_scale}' from a snapshot
        of scale '{snapshot_scale}'. Restoring snapshots to a differently sized
        cluster is not possible, you can either
        - Restart at the original scale and re-scale at runtime
        - Restart without loading this snapshot
    "
    )
}

#[cfg(test)]
mod test {

    use crate::{
        keyed::partitioners::index_select,
        snapshot::NoPersistence,
        testing::{OperatorTester, SentMessage},
    };

    use super::*;
    /// Bug I had, check the remote barrier is actually aligned and
    /// not just passed downstream directly
    #[test]
    fn remote_barrier_aligned() {
        let mut tester = OperatorTester::built_by(
            move |ctx| {
                let mut dist: Distributor<u64, (), i32> = Distributor::new(index_select, ctx);
                move |input, output, op_ctx| dist.run(input, output, op_ctx)
            },
            0,
            0,
            0..2,
        );

        tester.send_local(Message::Epoch(15));
        // should be none since we have no epoch from remote yet to align
        tester.step();
        assert!(tester.recv_local().is_none());
        tester
            .remote()
            .send_to_operator(NetworkMessage::<u64, (), i32>::Epoch(42), 1, 0);
        tester.step();

        // should be 15 since that is the lower alignment of both epochs
        match tester.recv_local() {
            Some(Message::Epoch(e)) => assert_eq!(e, 15),
            _ => panic!(),
        }
    }

    /// Epoch should be broadcasted to other workers
    #[test]
    fn epoch_is_broadcasted() {
        let mut tester: OperatorTester<u64, (), i32, u64, (), i32, NetworkMessage<u64, (), i32>> =
            OperatorTester::built_by(
                move |ctx| {
                    let mut dist = Distributor::new(index_select, ctx);
                    move |input, output, op_ctx| dist.run(input, output, op_ctx)
                },
                0,
                0,
                0..3,
            );

        let in_msg = Message::Epoch(22);
        tester.send_local(in_msg);
        tester.step();

        let out0 = tester.remote().recv_from_operator().unwrap();
        let out1 = tester.remote().recv_from_operator().unwrap();
        assert!(
            matches!(
                out0,
                SentMessage {
                    to_worker: 1,
                    to_operator: 0,
                    msg: NetworkMessage::Epoch(22)
                }
            ),
            "{out0:?}"
        );
        assert!(
            matches!(
                out1,
                SentMessage {
                    to_worker: 2,
                    to_operator: 0,
                    msg: NetworkMessage::Epoch(22)
                }
            ),
            "{out1:?}"
        );
    }

    /// A shutdown marker coming in from local upstream should be broadcasted and sent downstream
    #[test]
    fn broadcast_shutdown() {
        let mut tester: OperatorTester<u64, (), i32, u64, (), i32, NetworkMessage<u64, (), i32>> =
            OperatorTester::built_by(
                move |ctx| {
                    let mut dist = Distributor::new(index_select, ctx);
                    move |input, output, op_ctx| dist.run(input, output, op_ctx)
                },
                0,
                0,
                0..3,
            );
        tester.send_local(Message::SuspendMarker(SuspendMarker::default()));
        tester.step();

        let out0 = tester.remote().recv_from_operator().unwrap();
        let out1 = tester.remote().recv_from_operator().unwrap();
        assert!(
            matches!(
                out0,
                SentMessage {
                    to_worker: 1,
                    to_operator: 0,
                    msg: NetworkMessage::SuspendMarker
                }
            ),
            "{out0:?}"
        );
        assert!(
            matches!(
                out1,
                SentMessage {
                    to_worker: 2,
                    to_operator: 0,
                    msg: NetworkMessage::SuspendMarker
                }
            ),
            "{out1:?}"
        );
    }
    /// A barrier received from a local upstream should not trigger any output, when there is no state on the remote barrier
    #[test]
    fn align_barrier_from_local_none() {
        let mut tester: OperatorTester<u64, (), i32, u64, (), i32, NetworkMessage<u64, (), i32>> =
            OperatorTester::built_by(
                move |ctx| {
                    let mut dist = Distributor::new(index_select, ctx);
                    move |input, output, op_ctx| dist.run(input, output, op_ctx)
                },
                0,
                0,
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
        let mut tester: OperatorTester<u64, (), i32, u64, (), i32, NetworkMessage<u64, (), i32>> =
            OperatorTester::built_by(
                move |ctx| {
                    let mut dist = Distributor::new(index_select, ctx);
                    move |input, output, op_ctx| dist.run(input, output, op_ctx)
                },
                0,
                0,
                0..2,
            );
        tester
            .remote()
            .send_to_operator(NetworkMessage::BarrierMarker, 1, 0);
        tester.step();
        assert!(tester.recv_local().is_none());
    }
    /// A barrier received from a local upstream should trigger a barrier output when there is state
    /// for the remote
    #[test]
    fn align_barrier_from_local() {
        let mut tester: OperatorTester<u64, (), i32, u64, (), i32, NetworkMessage<u64, (), i32>> =
            OperatorTester::built_by(
                move |ctx| {
                    let mut dist = Distributor::new(index_select, ctx);
                    move |input, output, op_ctx| dist.run(input, output, op_ctx)
                },
                0,
                0,
                0..2,
            );
        tester
            .remote()
            .send_to_operator(NetworkMessage::BarrierMarker, 1, 0);
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
        let mut tester: OperatorTester<u64, (), i32, u64, (), i32, NetworkMessage<u64, (), i32>> =
            OperatorTester::built_by(
                move |ctx| {
                    let mut dist = Distributor::new(index_select, ctx);
                    move |input, output, op_ctx| dist.run(input, output, op_ctx)
                },
                0,
                0,
                0..2,
            );
        tester
            .remote()
            .send_to_operator(NetworkMessage::BarrierMarker, 1, 0);
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
        let mut tester: OperatorTester<u64, (), i32, u64, (), i32, NetworkMessage<u64, (), i32>> =
            OperatorTester::built_by(
                move |ctx| {
                    let mut dist = Distributor::new(index_select, ctx);
                    move |input, output, op_ctx| dist.run(input, output, op_ctx)
                },
                0,
                0,
                0..2,
            );
        tester.send_local(Message::AbsBarrier(Barrier::new(Box::new(
            NoPersistence::default(),
        ))));
        tester
            .remote()
            .send_to_operator(NetworkMessage::BarrierMarker, 1, 0);
        tester.step();
        let local_result = tester.recv_local().unwrap();
        assert!(
            matches!(local_result, Message::AbsBarrier(_)),
            "{local_result:?}"
        );
    }
    /// If we receive a suspend marker from a remote and that remote was previously holding back the
    /// advancement of the barrier, the barrier should advance after the remote has shut down
    #[test]
    fn advance_barrier_after_remote_shutdown() {
        let mut tester: OperatorTester<u64, (), i32, u64, (), i32, NetworkMessage<u64, (), i32>> =
            OperatorTester::built_by(
                move |ctx| {
                    let mut dist = Distributor::new(index_select, ctx);
                    move |input, output, op_ctx| dist.run(input, output, op_ctx)
                },
                0,
                0,
                0..2,
            );

        tester.send_local(Message::AbsBarrier(Barrier::new(Box::new(
            NoPersistence::default(),
        ))));

        tester
            .remote()
            .send_to_operator(NetworkMessage::SuspendMarker, 1, 0);
        tester.step();

        let advanced = tester.recv_local().unwrap();

        assert!(matches!(advanced, Message::AbsBarrier(_)));
    }

    /// It must not forward any data before the barriers are aligned
    #[test]
    fn no_barrier_overtaking_remote_barrier() {
        let mut tester: OperatorTester<
            u64,
            String,
            i32,
            u64,
            String,
            i32,
            NetworkMessage<u64, String, i32>,
        > = OperatorTester::built_by(
            move |ctx| {
                let mut dist = Distributor::new(index_select, ctx);
                move |input, output, op_ctx| dist.run(input, output, op_ctx)
            },
            0,
            0,
            0..2,
        );
        // send a barrier to "block" the operator from forwarding data
        tester
            .remote()
            .send_to_operator(NetworkMessage::BarrierMarker, 1, 0);
        tester.remote().send_to_operator(
            NetworkMessage::Data(NetworkDataMessage::new(
                DataMessage::new(1, "Hi".to_owned(), 1),
                0,
            )),
            1,
            0,
        );

        tester.step();
        tester.step();

        // this should be none since the operator will block until
        // it gets a barrier message from upstream too
        let msg = tester.recv_local();
        assert!(msg.is_none(), "{msg:?}");

        tester.send_local(Message::AbsBarrier(Barrier::new(Box::new(
            NoPersistence::default(),
        ))));
        tester.step();

        let barrier = tester.recv_local().unwrap();
        assert!(matches!(barrier, Message::AbsBarrier(_)));
    }

    /// It must not forward any data before the barriers are aligned
    #[test]
    fn no_barrier_overtaking_local_barrier() {
        let mut tester: OperatorTester<
            u64,
            String,
            i32,
            u64,
            String,
            i32,
            NetworkMessage<u64, String, i32>,
        > = OperatorTester::built_by(
            move |ctx| {
                let mut dist = Distributor::new(index_select, ctx);
                move |input, output, op_ctx| dist.run(input, output, op_ctx)
            },
            0,
            0,
            0..2,
        );

        tester.send_local(Message::AbsBarrier(Barrier::new(Box::new(
            NoPersistence::default(),
        ))));
        tester.send_local(Message::Data(DataMessage::new(0, "Hi".to_owned(), 10)));

        tester.step();
        assert!(tester.recv_local().is_none());

        tester
            .remote()
            .send_to_operator(NetworkMessage::BarrierMarker, 1, 0);
        tester.step();
        let barrier = tester.recv_local().unwrap();
        tester.step();
        let message = tester.recv_local().unwrap();
        assert!(matches!(barrier, Message::AbsBarrier(_)));
        assert!(matches!(message, Message::Data(_)));
    }
}
