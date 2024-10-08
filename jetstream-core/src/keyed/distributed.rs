
use indexmap::IndexMap;
use itertools::Itertools;

mod message_router;
pub mod types;

use std::iter::once;

use message_router::{MessageRouter, NormalRouter};
pub use types::*;

use crate::{
    channels::selective_broadcast::{Receiver, Sender},
    runtime::CommunicationClient,
    snapshot::Barrier,
    stream::{BuildContext, OperatorContext},
    types::{
        DataMessage, Message, RescaleMessage, ShutdownMarker, Timestamp,
        WorkerId,
    },
};

use crate::runtime::communication::broadcast;

type Remotes<K, V, T> =
    IndexMap<WorkerId, (CommunicationClient<NetworkMessage<K, V, T>>, RemoteState<T>)>;

pub(super) struct Distributor<K, V, T> {
    router: Container<MessageRouter<K, V, T>>,
    remotes: Remotes<K, V, T>,
    partitioner: WorkerPartitioner<K>,
    local_barrier: Option<Barrier>,
    local_shutdown: Option<ShutdownMarker>,
}

impl<K, V, T> Distributor<K, V, T>
where
    K: DistKey,
    V: DistData,
    T: DistTimestamp,
{
    pub(super) fn new(paritioner: WorkerPartitioner<K>, ctx: &mut BuildContext) -> Self {
        let snapshot: Option<(NormalRouter, IndexMap<WorkerId, RemoteState<T>>)> = ctx.load_state();
        let other_workers = ctx
            .get_worker_ids()
            .filter(|x| *x != ctx.worker_id)
            .collect_vec();

        let (state, remotes) = match snapshot {
            Some((local_state, remote_states)) => {
                // restoring from a differently sized snapshot is not supported
                if remote_states.len() != other_workers.len() {
                    // +1 to include this worker
                    panic_wrong_scale(ctx.get_worker_ids().len(), remote_states.len() + 1);
                }
                let remotes = create_remotes(&other_workers, ctx);
                (MessageRouter::Normal(local_state), remotes)
            }
            None => {
                let remotes = create_remotes(&other_workers, ctx);
                let state = MessageRouter::new(
                    ctx.get_worker_ids().collect(),
                    Version::default(),
                );
                (state, remotes)
            }
        };

        Self {
            router: Container::new(state),
            remotes,
            partitioner: paritioner,
            local_barrier: None,
            local_shutdown: None,
        }
    }

    /// Schedule this as an operator in the dataflow
    pub(super) fn run(
        &mut self,
        input: &mut Receiver<K, V, T>,
        output: &mut Sender<K, V, T>,
        ctx: &mut OperatorContext,
    ) {
        // HACK we collect messages into the vec because
        // we can't hold onto a &self when invoking the handlers
        let remote_message: Vec<(WorkerId, NetworkMessage<K, V, T>)> = self
            .remotes
            .iter()
            .filter(|(_wid, (_client, state))| !state.is_barred && !state.sent_shutdown)
            .filter_map(|(wid, (client, _state))| client.recv().map(|msg| (*wid, msg)))
            .collect();

        for (wid, msg) in remote_message.into_iter() {
            match msg {
                NetworkMessage::Data(data_message) => {
                    self.handle_remote_data_message(data_message, &wid, output, ctx)
                }
                NetworkMessage::Epoch(epoch) => {
                    self.remotes.get_mut(&wid).unwrap().1.frontier = Some(epoch.clone());
                    self.handle_local_epoch(epoch, output)
                }
                NetworkMessage::BarrierMarker => {
                    self.remotes.get_mut(&wid).unwrap().1.is_barred = true
                }
                NetworkMessage::ShutdownMarker => {
                    self.remotes.get_mut(&wid).unwrap().1.sent_shutdown = true
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
                    Message::Epoch(epoch) => self.handle_local_epoch(epoch, output),
                    Message::AbsBarrier(barrier) => self.handle_local_barrier(barrier),
                    Message::Rescale(rescale) => self.handle_rescale_message(rescale, output, ctx),
                    Message::ShutdownMarker(shutdown_marker) => {
                        self.local_shutdown = Some(shutdown_marker)
                    }

                    // these ones we can just ignore
                    Message::Interrogate(_) => (),
                    Message::Collect(_) => (),
                    Message::Acquire(_) => (),
                    Message::DropKey(_) => (),
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
            .apply(|x| x.lifecycle(self.partitioner, output, &self.remotes));
    }

    /// Handle a data message we received from our local upstream
    fn handle_local_data_message(
        &mut self,
        message: DataMessage<K, V, T>,
        output: &mut Sender<K, V, T>,
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
        output: &mut Sender<K, V, T>,
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
        output: &mut Sender<K, V, T>,

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
    fn handle_local_epoch(&self, epoch: T, output: &mut Sender<K, V, T>) {
        let wrapped_epoch = Some(epoch);
        let all_timestamps = self
            .remotes
            .values()
            .map(|x| &x.1.frontier)
            .chain(once(&wrapped_epoch));
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
        output: &mut Sender<K, V, T>,
        ctx: &mut OperatorContext,
    ) {
        match &message {
            // we can not remove these yet because they may still have messages for us
            RescaleMessage::ScaleRemoveWorker(_index_set) => (),
            RescaleMessage::ScaleAddWorker(to_add) => {
                for wid in to_add {
                    let comm_client = ctx.create_communication_client(*wid, ctx.worker_id);
                    let remote_state = RemoteState::default();
                    self.remotes.insert(*wid, (comm_client, remote_state));
                }
            }
        }

        self.router
            .apply(|router| router.handle_rescale(message, self.partitioner, output))
    }

    /// Emits a barrier to the output only and only if
    /// - we have one from our local upstream
    /// - we have one from every connected client
    #[inline]
    fn try_emit_barrier(&mut self, output: &mut Sender<K, V, T>) {
        if self.local_barrier.is_some() && self.remotes.values().all(|x| x.1.is_barred) {
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
    fn try_emit_shutdown(&mut self, output: &mut Sender<K, V, T>) {
        if self.local_shutdown.is_some() && self.remotes.values().all(|x| x.1.sent_shutdown) {
            let msg = Message::ShutdownMarker(self.local_shutdown.take().unwrap());
            output.send(msg);
        }
    }
}

fn create_remotes<K, V, T>(
    other_workers: &[WorkerId],
    ctx: &mut BuildContext,
) -> Remotes<K, V, T>
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
                    ctx.create_communication_client(*worker_id, ctx.operator_id),
                    RemoteState::default(),
                ),
            )
        })
        .collect();
    remotes
}

/// Small reducer hack, as we can't use iter::reduce because of ownership
fn merge_timestamps<'a, T: Timestamp>(
    mut timestamps: impl Iterator<Item = &'a Option<T>>,
) -> Option<T> {
    let mut merged = timestamps.next()?.clone();
    for x in timestamps {
        if let Some(y) = x {
            merged = merged.map(|a| a.merge(y));
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
