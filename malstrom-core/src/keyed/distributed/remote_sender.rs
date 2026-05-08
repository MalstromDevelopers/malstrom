use std::rc::Rc;

use indexmap::{IndexMap, IndexSet};

use crate::channels::operator_io::Output;
use crate::channels::recv_trait::Receiver as _;
use crate::keyed::distributed::routers::RouterOutput;
use crate::keyed::distributed::targeted_message::{TargetedData, TargetedMessage};
use crate::keyed::distributed::versioned_message::{VersionedData, VersionedMessage};
use crate::keyed::distributed::wire_message::{WireAcquire, WireMessage};
use crate::keyed::distributed::{Acquire, ConfigVersion};
use crate::runtime::OperatorOperatorComm;
use crate::runtime::communication::{OperatorCommSender, broadcast};
use crate::stream::{BuildContext, Logic, OperatorContext};
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
    pub(super) async fn new(ctx: &BuildContext) -> Self {
        let comm = ctx.get_communication();
        let mut remote_wids = ctx.get_worker_ids().to_owned();
        remote_wids.swap_remove(&ctx.worker_id);
        let mut remote_senders = IndexMap::new();

        for wid in remote_wids.iter() {
            let sender = OperatorCommSender::new(*wid, ctx.operator_id, &*comm)
                .await
                .expect("Communication Backend failed");
            remote_senders.insert(*wid, sender);
        }

        Self {
            remote_senders,
            comm: Rc::clone(&comm),
        }
    }

    pub(super) async fn send(
        &mut self,
        msg: TargetedMessage<M>,
        output: &mut Output<M>,
        ctx: &mut OperatorContext,
    ) {
        match msg {
            TargetedMessage::Data(data) => self.handle_data(data, output, ctx).await,
            TargetedMessage::Other(message) => match message {
                Message::Data(data_message) => unreachable!(),
                Message::Epoch(epoch) => self.handle_epoch(epoch, output).await,
                Message::Rescale(rescale) => self.handle_rescale(rescale, output, ctx).await,
                Message::ReconfigComplete(reconfig) => {
                    self.handle_reconfig_complete(reconfig, output).await
                }
                msg => output.send(msg).await,
            },
        }
    }

    async fn handle_data(
        &mut self,
        msg: TargetedData<M>,
        output: &mut Output<M>,
        ctx: &OperatorContext,
    ) {
        let target = msg.target_id;

        if target == ctx.worker_id {
            let local_msg = Message::Data(msg.data_msg);
            output.send(local_msg).await
        } else {
            let wire_msg = WireMessage::Data(VersionedData {
                sender_id: ctx.worker_id,
                config_version: msg.config_version,
                data_msg: msg.data_msg,
            });

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

    async fn handle_epoch(&mut self, epoch: M::Timestamp, output: &mut Output<M>) {
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
            let sender = OperatorCommSender::new(*wid, ctx.operator_id, self.comm.as_ref())
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
