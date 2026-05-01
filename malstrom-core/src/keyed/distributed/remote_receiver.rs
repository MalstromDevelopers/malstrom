use std::rc::Rc;

use futures::FutureExt;
use indexmap::{IndexMap, IndexSet};

use crate::channels::alignment::AlignedValue;
use crate::channels::operator_io::{Output, merge_timestamps};
use crate::channels::recv_trait::Receiver;
use crate::keyed::distributed::ConfigVersion;
use crate::keyed::distributed::versioned_message::VersionedMessage;
use crate::keyed::distributed::wire_message::WireMessage;
use crate::runtime::OperatorOperatorComm;
use crate::runtime::communication::{OperatorCommSender, broadcast};
use crate::stream::{Logic, OperatorContext};
use crate::types::distributable::Distributable;
use crate::types::{Key, ReconfigComplete, RescaleMessage};
use crate::{
    channels::{alignment::AlignmentGroup, operator_io::Input},
    runtime::communication::OperatorCommReceiver,
    types::{Barrier, Kvt, Message, WorkerId},
};

/// Aligns barriers from all remote receivers
type RemoteReceivers<M> = AlignmentGroup<WorkerId, ReceiverWrapper<M>, fn(&WireMessage<M>) -> bool>;

/// Type alias for a map of barrier senders indexed by worker ID
type BarrierSenders<M> = IndexMap<WorkerId, OperatorCommSender<WireMessage<M>>>;

/// An operator which receives messages from all remote workers
///
/// The DistributorReceiver is responsible for receiving messages from both local and remote
/// workers, handling barrier alignment between local and remote workers and merging epochs.
pub(super) struct DistributorReceiver<M>
where
    M: Kvt,
    M::Key: Distributable,
    M::Value: Distributable,
    M::Timestamp: Distributable,
{
    /// Remote receivers indexed by worker ID
    remote_recvs: RemoteReceivers<M>,
    /// Senders used to indicate to remote workers that we have received a barrier locally, so they
    /// can align
    barrier_senders: BarrierSenders<M>,
    /// Barrier alignment state machine
    barrier: BarrierAligner,
    /// Communication backend for inter-operator communication
    comm: Rc<dyn OperatorOperatorComm>,
}

impl<M> DistributorReceiver<M>
where
    M: Kvt,
    M::Key: Distributable,
    M::Value: Distributable,
    M::Timestamp: Distributable,
{
    /// Main processing method that handles both local and remote messages
    ///
    /// This method applies the distributor receiver logic by processing messages based on
    /// the current barrier alignment status. It handles three different states:
    /// - NotWaiting: Uses tokio::select to handle either local or remote messages
    /// - WaitingForLocal: Only processes local messages
    /// - WaitingForRemote: Only processes remote messages
    ///
    /// After processing a message, it handles epoch merging for epoch messages.
    ///
    /// # Arguments
    /// * `input` - The input stream for local messages
    /// * `ctx` - The operator context containing worker information
    ///
    /// # Returns
    /// * `Option<VersionedMessage<M>>` - The processed message, or None if no message was available
    pub(super) async fn recv(
        &mut self,
        input: &mut Input<M>,
        ctx: &OperatorContext,
    ) -> Option<VersionedMessage<M>> {
    let msg = match self.barrier.status() {
        AlignStatus::NotWaiting => {
            tokio::select! {
                msg = input.recv() => {
                    self.handle_local_message(msg, ctx).await
                }
                msg = self.remote_recvs.recv() => {
                    self.handle_remote_message(msg, ctx).await
                }
            }
        }
        AlignStatus::WaitingForLocal => {
            let msg = input.recv().await;
            self.handle_local_message(msg, ctx).await
        }
        AlignStatus::WatingForRemote => {
            let msg = self.remote_recvs.recv().await;
            self.handle_remote_message(msg, ctx).await
        }
    };

    self.merge_frontiers(msg).await
}

    /// Handles epoch messages by merging timestamps from all remote receivers
    ///
    /// This method processes epoch messages by merging timestamps from all remote
    /// receivers and returns the merged epoch message.
    ///
    /// # Arguments
    /// * `msg` - The message to process
    ///
    /// # Returns
    /// * `Option<VersionedMessage<M>>` - The processed message, or the original message if not an epoch
    async fn merge_frontiers(
        &mut self,
        msg: Option<VersionedMessage<M>>,
    ) -> Option<VersionedMessage<M>> {
        match msg {
            Some(VersionedMessage::Other(Message::Epoch(e))) => {
                let merged_epoch =
                    merge_timestamps(self.remote_recvs.values().map(|x| &x.last_epoch));
                merged_epoch
                    .map(Message::Epoch)
                    .map(VersionedMessage::Other)
            }
            x => x,
        }
    }

    async fn handle_local_message(
        &mut self,
        msg: Message<M>,
        ctx: &OperatorContext,
    ) -> Option<VersionedMessage<M>> {
        /// local version will be assigned downstream by distributor
        let msg = match msg {
            Message::AbsBarrier(b) => self.handle_abs_barrier(b).await.map(Message::AbsBarrier),
            Message::ReconfigComplete(r) => Some(Message::ReconfigComplete(
                self.handle_reconfig_complete(r).await,
            )),
            Message::Rescale(r) => Some(Message::Rescale(self.handle_rescale(r, ctx).await)),
            x => Some(x),
        };
        msg.map(|msg| VersionedMessage::from_local_msg(msg, ctx.worker_id))
    }

    async fn handle_remote_message(
        &mut self,
        msg: AlignedValue<WorkerId, WireMessage<M>>,
        ctx: &OperatorContext,
    ) -> Option<VersionedMessage<M>> {
        let (sender, wire_message) = match msg {
            AlignedValue::Unaligned(wire_message) => wire_message,
            // a barrier from remote
            AlignedValue::Aligned(_) => return self.barrier.store_remote().map(Message::AbsBarrier)
                .map(VersionedMessage::Other)
        };

        match wire_message {
            WireMessage::Data(versioned_data) => Some(VersionedMessage::Data(versioned_data)),
            WireMessage::Epoch(e) => {
                self.remote_recvs
                    .get_mut(&sender)
                    .expect("sender ID must be valid")
                    .last_epoch = Some(e.clone());
                Some(VersionedMessage::Other(Message::Epoch(e)))
            }
            WireMessage::Acquire(wire_acquire) => Some(VersionedMessage::Other(Message::Acquire(
                wire_acquire.into(),
            ))),
            WireMessage::SnapshotBarrier => unreachable!("Barriers are aligned in AlignmentGroup")
        }
    }

    /// Handles barrier messages
    ///
    /// Informs remote workers about locally received barrier.
    /// Stores the local barrier and emits it to the output stream if both local
    /// and remote barriers have been received (alignment).
    ///
    /// # Arguments
    /// * `barrier` - The barrier message to process
    /// * `output` - The output stream to send the barrier to when aligned
    async fn handle_abs_barrier(&mut self, barrier: Barrier) -> Option<Barrier> {
        let msg = WireMessage::SnapshotBarrier;
        broadcast(self.barrier_senders.values(), msg).await;
        self.barrier.store_local(barrier)
    }

    /// Handles reconfiguration complete messages
    ///
    /// Updates both remote receivers and barrier senders to match the new worker set
    /// from the reconfiguration event, removing connections to workers that are no
    /// longer part of the system.
    ///
    /// # Arguments
    /// * `reconfig` - The reconfiguration complete message containing the new worker set
    /// * `output` - The output stream to forward the reconfiguration message to
    async fn handle_reconfig_complete(&mut self, reconfig: ReconfigComplete) -> ReconfigComplete {
        let workers = reconfig.get_new_worker_set();
        self.remote_recvs.retain(|wid| workers.contains(wid));
        self.barrier_senders.retain(|wid, _| workers.contains(wid));
        reconfig
    }

    /// Handles rescale messages by establishing connections to new workers
    ///
    /// When the system scales up, this method creates new receiver connections and
    /// barrier sender connections to any workers that have been added to the system
    /// but don't yet have established connections.
    ///
    /// # Arguments
    /// * `rescale` - The rescale message containing the complete set of workers
    /// * `output` - The output stream to forward the rescale message to
    /// * `ctx` - The operator context needed to create new receiver and sender connections
    async fn handle_rescale(
        &mut self,
        rescale: RescaleMessage,
        ctx: &OperatorContext,
    ) -> RescaleMessage {
        let all_workers = rescale.get_all_workers();
        let existing_workers: IndexSet<WorkerId> = self.remote_recvs.keys().map(|x| *x).collect();
        let new_workers = all_workers.difference(&existing_workers);
        for wid in new_workers.into_iter() {
            let receiver =
                OperatorCommReceiver::new(ctx.worker_id, ctx.operator_id, self.comm.as_ref())
                    .await
                    .expect("Communication backend failure");
            let receiver = ReceiverWrapper {
                receiver,
                last_epoch: None,
            };
            self.remote_recvs.insert(*wid, receiver);

            let sender = OperatorCommSender::new(*wid, ctx.operator_id, self.comm.as_ref())
                .await
                .expect("Communication backend failure");
            self.barrier_senders.insert(*wid, sender);
        }
        rescale
    }
}

struct ReceiverWrapper<M: Kvt> {
    receiver: OperatorCommReceiver<WireMessage<M>>,
    last_epoch: Option<M::Timestamp>,
}
impl<M> Receiver for ReceiverWrapper<M>
where
    M: Kvt,
    M::Key: Distributable,
    M::Value: Distributable,
    M::Timestamp: Distributable,
{
    type Output = WireMessage<M>;

    async fn recv(&mut self) -> Self::Output {
        self.receiver.recv().await
    }
}

/// State machine for aligning local and remote barriers
///
/// The BarrierAligner ensures that barriers are only emitted when both local
/// and remote barriers have been received.
struct BarrierAligner {
    /// local barrier if we have received it yet
    local_barrier: Option<Barrier>,
    /// whether or not we have received the remote barrier yet
    got_remote: bool,
}
impl BarrierAligner {
    /// Stores a locally received barrier and attempts to emit it if alignment is achieved
    ///
    /// This method stores the local barrier and checks if both local and remote
    /// barriers have been received. If so, it returns the barrier for emission.
    ///
    /// # Arguments
    /// * `barrier` - The local barrier to store
    ///
    /// # Returns
    /// * `Some(Barrier)` if both local and remote barriers are available (aligned)
    /// * `None` if still waiting for the remote barrier
    fn store_local(&mut self, barrier: Barrier) -> Option<Barrier> {
        let prev_local = self.local_barrier.replace(barrier);
        debug_assert!(
            prev_local.is_none(),
            "Received multiple local barriers in succession"
        );

        self.try_take()
    }

    /// Indicates that a remote barrier was received and attempts to emit the stored local barrier
    ///
    /// This method marks that a remote barrier has been received and checks if
    /// a local barrier was previously stored. If so, it returns the local barrier for emission.
    ///
    /// # Returns
    /// * `Some(Barrier)` if a local barrier was previously stored (aligned)
    /// * `None` if still waiting for the local barrier
    fn store_remote(&mut self) -> Option<Barrier> {
        debug_assert!(
            !self.got_remote,
            "Got multiple remote barriers in succession"
        );
        self.got_remote = true;
        self.try_take()
    }

    /// Attempts to take out a barrier if both local and remote barriers have been received
    ///
    /// This method checks if alignment has been achieved (both barriers received)
    /// and if so, takes the local barrier and resets the state machine.
    ///
    /// # Returns
    /// * `Some(Barrier)` if alignment is achieved
    /// * `None` if still waiting for one or both barriers
    fn try_take(&mut self) -> Option<Barrier> {
        match self.local_barrier.take_if(|_| self.got_remote) {
            Some(x) => {
                self.got_remote = false;
                Some(x)
            }
            None => None,
        }
    }

    /// Gets the current alignment status of the barrier aligner
    ///
    /// # Returns
    /// The current `AlignStatus` indicating what the state machine is waiting for
    fn status(&self) -> AlignStatus {
        match (self.local_barrier.is_some(), self.got_remote) {
            (false, false) => AlignStatus::NotWaiting,
            (true, false) => AlignStatus::WatingForRemote,
            (false, true) => AlignStatus::WaitingForLocal,
            (true, true) => unreachable!("Barrier should have been emitted"),
        }
    }
}

/// Alignment status indicating what the barrier aligner is waiting for
enum AlignStatus {
    /// Neither waiting for a remote nor a local barrier
    NotWaiting,
    /// Waiting for a local barrier to arrive, already got remote
    WaitingForLocal,
    /// Waiting for a remote barrier to arrive, already got local
    WatingForRemote,
}
