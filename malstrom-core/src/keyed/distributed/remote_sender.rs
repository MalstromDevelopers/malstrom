use std::rc::Rc;

use indexmap::{IndexMap, IndexSet};

use crate::channels::operator_io::Output;
use crate::channels::recv_trait::Receiver as _;
use crate::keyed::distributed::versioned_message::{VersionedData, VersionedMessage};
use crate::keyed::distributed::wire_message::{WireAcquire, WireMessage};
use crate::keyed::distributed::{Acquire, ConfigVersion};
use crate::runtime::OperatorOperatorComm;
use crate::runtime::communication::{OperatorCommSender, broadcast};
use crate::stream::{Logic, OperatorContext};
use crate::types::distributable::Distributable;
use crate::types::{DataMessage, Key, OperatorId, ReconfigComplete, RescaleMessage};
use crate::{
    channels::{alignment::AlignmentGroup, operator_io::Input},
    runtime::communication::OperatorCommReceiver,
    types::{Barrier, Kvt, Message, WorkerId},
};

type RemoteSenders<M> = IndexMap<WorkerId, OperatorCommSender<WireMessage<M>>>;

/// An operator which receives messages from all remote workers
pub(super) struct DistributorSender<M>
where
    // TODO: simplify these trait bounds
    M: Kvt,
    M::Key: Distributable,
    M::Value: Distributable,
    M::Timestamp: Distributable,
{
    /// Remote sender indexed by worker ID
    remote_senders: RemoteSenders<M>,
    /// Operator ID of the receiver operator
    receiver_operator_id: OperatorId,
    /// Communication backend for inter-operator communication
    comm: Rc<dyn OperatorOperatorComm>,
}

impl<M> DistributorSender<M>
where
    M: Kvt + Distributable,
    M::Key: Distributable,
    M::Value: Distributable,
    M::Timestamp: Distributable,
{
    async fn handle_data(
        &mut self,
        target: WorkerId,
        msg: VersionedData<M>,
        output: &mut Output<M>,
        ctx: OperatorContext,
    ) {
        if target == ctx.worker_id {
            let local_msg = Message::Data(msg.data_msg);
            output.send(local_msg).await
        } else {
            let wire_msg = WireMessage::Data(msg);
            let client = self
                .remote_senders
                .get(&target)
                .expect("Expected message target to be valid");
            client.send(wire_msg).await
        }
    }

    async fn handle_acquire(
        &mut self,
        target: WorkerId,
        msg: WireAcquire<M::Key>,
        output: &mut Output<M>,
        ctx: OperatorContext,
    ) {
        if target == ctx.worker_id {
            let acquire = Acquire::from(msg);
            let local_msg = Message::Acquire(acquire);
            output.send(local_msg).await
        } else {
            let client = self
                .remote_senders
                .get(&target)
                .expect("Expected message target to be valid");
            client.send(WireMessage::Acquire(msg.into())).await
        }
    }

    async fn handle_epoch(
        &mut self,
        epoch: M::Timestamp,
        output: &mut Output<M>,
        ctx: OperatorContext,
    ) {
        broadcast(
            self.remote_senders.values(),
            WireMessage::Epoch(epoch.clone()),
        )
        .await;
        output.send(Message::Epoch(epoch)).await
    }

    async fn handle_rescale(
        &mut self,
        rescale: RescaleMessage,
        output: &mut Output<M>,
        ctx: &mut OperatorContext,
    ) {
        let all_workers = rescale.get_all_workers();
        let existing_workers: IndexSet<WorkerId> = self.remote_senders.keys().map(|x| *x).collect();
        let new_workers = all_workers.difference(&existing_workers);

        for wid in new_workers.into_iter() {
            let sender =
                OperatorCommSender::new(*wid, self.receiver_operator_id, self.comm.as_ref())
                    .await
                    .expect("Communication backend failure");
            self.remote_senders.insert(*wid, sender);
        }
        output.send(Message::Rescale(rescale)).await
    }

    async fn handle_reconfig_complete(
        &mut self,
        reconfig: ReconfigComplete,
        output: &mut Output<M>,
    ) {
        let workers = reconfig.get_new_worker_set();
        self.remote_senders.retain(|wid, _| workers.contains(wid));
        output.send(Message::ReconfigComplete(reconfig)).await
    }
}
