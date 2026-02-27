use futures::{StreamExt, stream::FuturesUnordered};
use indexmap::IndexMap;
use itertools::Itertools;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

mod message_router;
pub mod types;
mod routers;

use std::iter::once;

use message_router::{MessageRouter, NormalRouter};
// pub use types::*;

// mod remote_receiver;
mod wire_message;
mod distributor;
mod reconfig_task;

use crate::{
    channels::operator_io::{Input, Output},
    runtime::BiCommunicationClient,
    snapshot::SnapshotBarrier,
    stream::{BuildContext, Logic, OperatorContext},
    types::{DataMessage, Key, Kvt, MaybeTime, Message, RescaleMessage, SuspendMarker, WorkerId},
};

use crate::runtime::communication::broadcast;

type Remotes<M: Kvt> = IndexMap<
    WorkerId,
    (
        BiCommunicationClient<NetworkMessage<M>>,
        RemoteState<<M as Kvt>::Timestamp>,
    ),
>;

pub(crate) struct Distributor<M: Kvt> {
    router: Container<MessageRouter<M>>,
    remotes: Remotes<M>,
    partitioner: WorkerPartitioner<<M as Kvt>::Key>,
    local_barrier: Option<SnapshotBarrier>,
    local_suspend: Option<SuspendMarker>,
    local_frontier: Option<<M as Kvt>::Timestamp>,
}

impl<M> Logic<M, M> for Distributor<M>
where
    M: Kvt,
    M::Key: Key + Serialize + DeserializeOwned,
    M::Value: Serialize + DeserializeOwned,
    M::Timestamp: Serialize + DeserializeOwned,
{
    async fn apply(
        &mut self,
        input: &mut Input<M>,
        output: &mut Output<M>,
        ctx: &mut OperatorContext,
    ) {
        let locally_blocked = self.local_barrier.is_some() || self.local_suspend.is_some();
        let mut remote_msgs: FuturesUnordered<_> = self
            .remotes
            .iter()
            .filter(|(_wid, (_client, state))| !state.is_barred && !state.sent_suspend)
            .map(|(wid, (client, _state))| async { (*wid, client.recv_async().await) })
            .collect();

        let mut handle_remote = async |wid, msg, dist: &mut Self| match msg {
            NetworkMessage::Data(data_message) => {
                dist.handle_remote_data_message(data_message, &wid, output, ctx)
                    .await
            }
            NetworkMessage::Epoch(epoch) => {
                dist.remotes.get_mut(&wid).unwrap().1.frontier = Some(epoch.clone());
                dist.handle_epoch(output).await
            }
            NetworkMessage::BarrierMarker => dist.remotes.get_mut(&wid).unwrap().1.is_barred = true,
            NetworkMessage::SuspendMarker => {
                dist.remotes.get_mut(&wid).unwrap().1.sent_suspend = true
            }
            NetworkMessage::Acquire(network_acquire) => {
                output.send(Message::Acquire(network_acquire.into())).await
            }
            NetworkMessage::Upgrade(version) => {
                let remote = dist.remotes.get_mut(&wid).unwrap();
                remote.1.last_version = Some(version);
                remote.0.send(NetworkMessage::AckUpgrade(version));
            }
            NetworkMessage::AckUpgrade(version) => {
                dist.remotes.get_mut(&wid).unwrap().1.last_ack_version = Some(version);
            }
        };

        // need to drop remote_msgs to allow &mut access to self
        if locally_blocked {
            let remote_msg = remote_msgs.next().await;
            drop(remote_msgs);
            if let Some((wid, msg)) = remote_msg {
                handle_remote(wid, msg, self).await
            }
        } else {
            tokio::select! {
                Some((wid, msg)) = remote_msgs.next() => {
                    drop(remote_msgs);
                    handle_remote(wid, msg, self).await
                },
                msg = input.recv() => {
                    drop(remote_msgs);
                    match msg {
                        Message::Data(msg) => self.handle_local_data_message(msg, output, ctx).await,
                        Message::Epoch(epoch) => {
                            // TODO must not allow epochs to overtake messages while rescaling
                            broadcast(
                                self.remotes.values().map(|x| &x.0),
                                NetworkMessage::Epoch(epoch.clone()),
                            );
                            self.local_frontier = Some(epoch);
                            self.handle_epoch(output).await
                        }
                        Message::AbsBarrier(barrier) => self.handle_local_barrier(barrier),
                        Message::Rescale(rescale) => self.handle_rescale_message(rescale, output, ctx).await,
                        Message::SuspendMarker(shutdown_marker) => {
                            self.local_suspend = Some(shutdown_marker);
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
        }

        // try to clear these
        // Now you might be tempted to do this in an event driven way
        // where we only call these functions if we get shutdown or barrier
        // messages, but that does not handle the case where the removal
        // of another worker allows them to be emitted
        // Maybe in the future we will solve this smarter, but for now
        // I am leaving this here
        self.try_clear_barrier(output).await;
        self.try_clear_suspend(output).await;
        self.router
            .apply(async |x| {
                x.lifecycle(self.partitioner, output, &mut self.remotes)
                    .await
            })
            .await;
    }
}

type DistributorState<T> = (NormalRouter, IndexMap<WorkerId, RemoteState<T>>, Option<T>);
impl<M> Distributor<M>
where
    M: Kvt,
    M::Key: Key + Serialize + DeserializeOwned,
    M::Value: Serialize + DeserializeOwned,
    M::Timestamp: Serialize + DeserializeOwned,
{
    pub(super) async fn new(
        paritioner: WorkerPartitioner<<M as Kvt>::Key>,
        ctx: &mut BuildContext,
    ) -> Self {
        let snapshot: Option<DistributorState<<M as Kvt>::Timestamp>> = ctx.load_state().await;
        let other_workers = ctx
            .get_worker_ids()
            .iter()
            .copied()
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
                    ctx.get_worker_ids().iter().copied().collect(),
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
            local_suspend: None,
            local_frontier: frontier,
        }
    }

    /// Handle a data message we received from our local upstream
    async fn handle_local_data_message(
        &mut self,
        message: DataMessage<M>,
        output: &mut Output<M>,
        ctx: &OperatorContext,
    ) {
        let routing = {
            self.router.route_message(
                message,
                None,
                self.partitioner,
                ctx.worker_id,
                ctx.worker_id,
                &self.remotes,
            )
        };
        if let Some((msg, target)) = routing {
            self.send_data_message(msg, target, output, ctx).await;
        }
    }

    async fn handle_remote_data_message(
        &mut self,
        message: NetworkDataMessage<M>,
        sent_by: &WorkerId,
        output: &mut Output<M>,
        ctx: &OperatorContext,
    ) {
        let routing = {
            self.router.route_message(
                message.content,
                Some(message.version),
                self.partitioner,
                ctx.worker_id,
                *sent_by,
                &self.remotes,
            )
        };
        if let Some((msg, target)) = routing {
            self.send_data_message(msg, target, output, ctx).await;
        }
    }

    async fn send_data_message(
        &self,
        message: DataMessage<M>,
        target: WorkerId,
        output: &mut Output<M>,
        ctx: &OperatorContext,
    ) {
        match target == ctx.worker_id {
            true => output.send(Message::Data(message)).await,
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
    async fn handle_epoch(&self, output: &mut Output<M>) {
        let all_timestamps = self
            .remotes
            .values()
            .map(|x| &x.1.frontier)
            .chain(once(&self.local_frontier));
        let merged = merge_timestamps(all_timestamps);
        if let Some(to_emit) = merged {
            output.send(Message::Epoch(to_emit)).await;
        }
    }

    /// Handle a barrier we receive
    fn handle_local_barrier(&mut self, barrier: SnapshotBarrier) {
        self.local_barrier = Some(barrier);
        broadcast(
            self.remotes.values().map(|x| &x.0),
            NetworkMessage::BarrierMarker,
        );
    }

    async fn handle_rescale_message(
        &mut self,
        message: RescaleMessage,
        output: &mut Output<M>,
        ctx: &mut OperatorContext,
    ) {
        // we can not remove clients of workers here because we need them during the rescale
        // process. They are removed in the final step of the "Finished" distributor
        for wid in message.get_new_workers() {
            if (!self.remotes.contains_key(wid)) && (*wid != ctx.worker_id) {
                let comm_client = ctx.create_communication_client(*wid);
                let remote_state = RemoteState::default();
                self.remotes.insert(*wid, (comm_client, remote_state));
            }
        }
        self.router
            .apply(async |router| {
                router
                    .handle_rescale(message, self.partitioner, output)
                    .await
            })
            .await
    }

    /// Emits a barrier to the output only and only if
    /// - we have one from our local upstream
    /// - we have one from every connected client
    #[inline]
    async fn try_clear_barrier(&mut self, output: &mut Output<M>) {
        if self.local_barrier.is_some()
            && self
                .remotes
                .values()
                .all(|x| x.1.is_barred || x.1.sent_suspend)
        {
            #[allow(clippy::unwrap_used)] // Safe because we just checked is_some
            let msg = Message::AbsBarrier(self.local_barrier.take().unwrap());
            output.send(msg).await;

            for (_, remote_state) in self.remotes.iter_mut().map(|x| x.1) {
                remote_state.is_barred = false;
            }
        }
    }

    /// Emits a shutdown to the output only and only if
    /// - we have one from our local upstream
    /// - we have one from every connected client
    #[inline]
    async fn try_clear_suspend(&mut self, output: &mut Output<M>) {
        if self.local_suspend.is_some() && self.remotes.values().all(|x| x.1.sent_suspend) {
            // can unwrap because we just checked is_some
            #[allow(clippy::unwrap_used)]
            let msg = Message::SuspendMarker(self.local_suspend.take().unwrap());
            output.send(msg).await;
        }
    }
}

fn create_remotes<M>(other_workers: &[WorkerId], ctx: &mut BuildContext) -> Remotes<M>
where
    M: Kvt,
    M::Key: Serialize + DeserializeOwned,
    M::Value: Serialize + DeserializeOwned,
    M::Timestamp: Serialize + DeserializeOwned,
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

    use crate::keyed::key_distribute::DistributorBuilder;
    use crate::stream::Logic as _;
    use crate::{
        keyed::partitioners::index_select,
        snapshot::NoPersistence,
        testing::{OperatorTester, SentMessage},
    };

    use super::*;
    /// Bug I had, check the remote barrier is actually aligned and
    /// not just passed downstream directly
    #[tokio::test]
    async fn remote_barrier_aligned() {
        type Msg = (u64, (), i32);
        let mut tester =
            OperatorTester::built_by(DistributorBuilder::<_, Msg>::new(index_select), 0, 0, 0..2)
                .await;

        tester.send_local(Message::Epoch(15));
        tester.send_local(Message::Epoch(15));
        // should be none since we have no epoch from remote yet to align
        tester.step();
        assert!(tester.recv_local().is_none());
        tester
            .remote()
            .send_to_operator(NetworkMessage::<Msg>::Epoch(42), 1, 0);
        tester.step();

        // should be 15 since that is the lower alignment of both epochs
        match tester.recv_local() {
            Some(Message::Epoch(e)) => assert_eq!(e, 15),
            _ => panic!(),
        }
    }

    /// Epoch should be broadcasted to other workers
    #[tokio::test]
    async fn epoch_is_broadcasted() {
        type Msg = (u64, (), i32);
        let mut tester: OperatorTester<Msg, Msg, _, NetworkMessage<Msg>> =
            OperatorTester::built_by(DistributorBuilder::<_, Msg>::new(index_select), 0, 0, 0..3)
                .await;

        let in_msg = Message::Epoch(22);
        tester.send_local(in_msg);
        tester.step();

        let out0 = tester.remote().recv_from_operator().unwrap();
        let out1 = tester.remote().recv_from_operator().unwrap();
        assert!(matches!(
            out0,
            SentMessage {
                to_worker: 1,
                to_operator: 0,
                msg: NetworkMessage::Epoch(22)
            }
        ));
        assert!(matches!(
            out1,
            SentMessage {
                to_worker: 2,
                to_operator: 0,
                msg: NetworkMessage::Epoch(22)
            }
        ));
    }

    /// A shutdown marker coming in from local upstream should be broadcasted and sent downstream
    #[tokio::test]
    async fn broadcast_shutdown() {
        type Msg = (u64, (), i32);
        let mut tester: OperatorTester<Msg, Msg, _, NetworkMessage<Msg>> =
            OperatorTester::built_by(DistributorBuilder::<_, Msg>::new(index_select), 0, 0, 0..3)
                .await;
        tester.send_local(Message::SuspendMarker(SuspendMarker::default()));
        tester.step();

        let out0 = tester.remote().recv_from_operator().unwrap();
        let out1 = tester.remote().recv_from_operator().unwrap();
        assert!(matches!(
            out0,
            SentMessage {
                to_worker: 1,
                to_operator: 0,
                msg: NetworkMessage::SuspendMarker
            }
        ));
        assert!(matches!(
            out1,
            SentMessage {
                to_worker: 2,
                to_operator: 0,
                msg: NetworkMessage::SuspendMarker
            }
        ));
    }
    /// A barrier received from a local upstream should not trigger any output, when there is no state on the remote barrier
    #[tokio::test]
    async fn align_barrier_from_local_none() {
        type Msg = (u64, (), i32);
        let mut tester: OperatorTester<Msg, Msg, _, NetworkMessage<Msg>> =
            OperatorTester::built_by(DistributorBuilder::<_, Msg>::new(index_select), 0, 0, 0..2)
                .await;
        tester.send_local(Message::AbsBarrier(SnapshotBarrier::new(Box::new(NoPersistence))));
        tester.step();

        assert!(tester.recv_local().is_none());
    }
    /// A barrier received from a remote should not trigger any output, when there is no state on the local barrier
    #[tokio::test]
    async fn align_barrier_from_remote_none() {
        type Msg = (u64, (), i32);
        let mut tester: OperatorTester<Msg, Msg, _, NetworkMessage<Msg>> =
            OperatorTester::built_by(DistributorBuilder::<_, Msg>::new(index_select), 0, 0, 0..2)
                .await;
        tester
            .remote()
            .send_to_operator(NetworkMessage::BarrierMarker, 1, 0);
        tester.step();
        assert!(tester.recv_local().is_none());
    }
    /// A barrier received from a local upstream should trigger a barrier output when there is state
    /// for the remote
    #[tokio::test]
    async fn align_barrier_from_local() {
        type Msg = (u64, (), i32);
        let mut tester: OperatorTester<Msg, Msg, _, NetworkMessage<Msg>> =
            OperatorTester::built_by(DistributorBuilder::<_, Msg>::new(index_select), 0, 0, 0..2)
                .await;
        tester
            .remote()
            .send_to_operator(NetworkMessage::BarrierMarker, 1, 0);
        tester.send_local(Message::AbsBarrier(SnapshotBarrier::new(Box::new(NoPersistence))));

        tester.step();
        let local_result = tester.recv_local().unwrap();
        assert!(
            matches!(local_result, Message::AbsBarrier(_)),
            "{local_result:?}"
        );
    }
    /// A barrier received from a local upstream should trigger a barrier output when there is state
    /// for the remote but the next barrier should need to be aligned again
    #[tokio::test]
    async fn align_barrier_from_local_twice() {
        type Msg = (u64, (), i32);
        let mut tester: OperatorTester<Msg, Msg, _, NetworkMessage<Msg>> =
            OperatorTester::built_by(DistributorBuilder::<_, Msg>::new(index_select), 0, 0, 0..2)
                .await;
        tester
            .remote()
            .send_to_operator(NetworkMessage::BarrierMarker, 1, 0);
        tester.send_local(Message::AbsBarrier(SnapshotBarrier::new(Box::new(NoPersistence))));

        tester.step();
        let local_result = tester.recv_local().unwrap();
        assert!(
            matches!(local_result, Message::AbsBarrier(_)),
            "{local_result:?}"
        );
        tester.send_local(Message::AbsBarrier(SnapshotBarrier::new(Box::new(NoPersistence))));
        tester.step();
        assert!(tester.recv_local().is_none());
    }
    /// A barrier received from a remote should trigger a barrier output when there is state
    /// for the local barrier
    #[tokio::test]
    async fn align_barrier_from_remote() {
        type Msg = (u64, (), i32);
        let mut tester: OperatorTester<Msg, Msg, _, NetworkMessage<Msg>> =
            OperatorTester::built_by(DistributorBuilder::<_, Msg>::new(index_select), 0, 0, 0..2)
                .await;
        tester.send_local(Message::AbsBarrier(SnapshotBarrier::new(Box::new(NoPersistence))));
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
    #[tokio::test]
    async fn advance_barrier_after_remote_shutdown() {
        type Msg = (u64, (), i32);
        let mut tester: OperatorTester<Msg, Msg, _, NetworkMessage<Msg>> =
            OperatorTester::built_by(DistributorBuilder::<_, Msg>::new(index_select), 0, 0, 0..2)
                .await;
        tester.send_local(Message::AbsBarrier(SnapshotBarrier::new(Box::new(NoPersistence))));

        tester
            .remote()
            .send_to_operator(NetworkMessage::SuspendMarker, 1, 0);
        tester.step();

        let advanced = tester.recv_local().unwrap();

        assert!(matches!(advanced, Message::AbsBarrier(_)));
    }

    /// It must not forward any data before the barriers are aligned
    #[tokio::test]
    async fn no_barrier_overtaking_remote_barrier() {
        type Msg = (u64, String, i32);
        let mut tester: OperatorTester<Msg, Msg, _, NetworkMessage<Msg>> =
            OperatorTester::built_by(DistributorBuilder::<_, Msg>::new(index_select), 0, 0, 0..2)
                .await;
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

        tester.send_local(Message::AbsBarrier(SnapshotBarrier::new(Box::new(NoPersistence))));
        tester.step();

        let barrier = tester.recv_local().unwrap();
        assert!(matches!(barrier, Message::AbsBarrier(_)));
    }

    /// It must not forward any data before the barriers are aligned
    #[tokio::test]
    async fn no_barrier_overtaking_local_barrier() {
        type Msg = (u64, String, i32);
        let mut tester: OperatorTester<Msg, Msg, _, NetworkMessage<Msg>> =
            OperatorTester::built_by(DistributorBuilder::<_, Msg>::new(index_select), 0, 0, 0..2)
                .await;

        OperatorTester::send_local(
            &mut tester,
            Message::AbsBarrier(SnapshotBarrier::new(Box::new(NoPersistence))),
        );
        OperatorTester::send_local(
            &mut tester,
            Message::Data(DataMessage::new(0, "Hi".to_owned(), 10)),
        );

        OperatorTester::step(&mut tester);
        assert!(OperatorTester::recv_local(&mut tester).is_none());

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
