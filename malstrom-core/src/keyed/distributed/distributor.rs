use std::{collections::VecDeque, hash::Hash};

use indexmap::{IndexMap, IndexSet};
use tokio::sync::oneshot;

use crate::{
    channels::{operator_io::{Input, Output}, recv_trait::Receiver, spsc},
    keyed::{
        WorkerPartitioner,
        distributed::{
            Collect, ConfigVersion, Interrogate, remote_receiver::DistributorReceiver, remote_sender::DistributorSender, targeted_message::TargetedData, versioned_message::{VersionedData, VersionedMessage}, wire_message::WireAcquire
        },
    },
    stream::{BuildContext, Logic, OperatorContext},
    types::{
        DataMessage, Key, Kvt, OperatorId, ReconfigComplete, RescaleMessage, WorkerId, distributable::Distributable
    },
};

struct Distributor<M>
where
    M: Kvt,
    M::Key: Distributable,
    M::Value: Distributable,
    M::Timestamp: Distributable,
{
    remote_receiver: DistributorReceiver<M>,
    remote_sender: DistributorSender<M>,
    router: MessageRouter<M>,
}

impl<M> Logic<M, M> for Distributor<M>
where
    M: Kvt,
    M::Key: Distributable,
    M::Value: Distributable,
    M::Timestamp: Distributable,
{
    async fn apply(
        &mut self,
        input: &mut Input<M>,
        output: &mut Output<M>,
        ctx: &mut OperatorContext,
    ) {
        let (collect, acquire) = self.router.run().await;
        let msg = self.remote_receiver.recv(input, ctx).await;
    }
}
