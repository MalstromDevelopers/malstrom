use std::{collections::VecDeque, hash::Hash};

use indexmap::{IndexMap, IndexSet};
use tokio::sync::oneshot;

use crate::{
    channels::{
        operator_io::{Input, Output},
        recv_trait::Receiver,
        spsc,
    },
    keyed::{
        WorkerPartitioner,
        distributed::{
            Collect, ConfigVersion, Interrogate,
            remote_receiver::DistributorReceiver,
            remote_sender::DistributorSender,
            routers::{MessageRouter, RouterInput, RouterOutput},
            targeted_message::{TargetedData, TargetedMessage},
            versioned_message::{VersionedData, VersionedMessage},
            wire_message::WireAcquire,
        },
    },
    stream::{BuildContext, Logic, LogicBuilder, OperatorContext},
    types::{
        DataMessage, Key, Kvt, Message, OperatorId, ReconfigComplete, RescaleMessage, WorkerId,
        distributable::Distributable,
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
    M: Kvt + Distributable,
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
        tokio::select! {
            input_msg = self.remote_receiver.recv(input, ctx) => {
                let input_msg = match input_msg {
                    Some(m) => m,
                    None => return,
                };
                match input_msg {
                    VersionedMessage::Data(versioned_data) => {
                        self.router.input.send(RouterInput::DataMessage(versioned_data)).await;
                    },
                    VersionedMessage::Other(message) => match message {
                        Message::Rescale(r) => self.router.input.send(RouterInput::Rescale(r)).await,
                        Message::ReconfigComplete(c) => self.router.input.send(RouterInput::Complete(c)).await,
                        Message::Epoch(e) => self.remote_sender.send(TargetedMessage::Other(Message::Epoch(e)), output, ctx).await,
                        Message::AbsBarrier(_) => (),
                        Message::Interrogate(_) => (),
                        Message::Collect(_) => (),
                        Message::Acquire(_) => (),
                        Message::Data(_) => unreachable!(),
                    },
                }
            }

            output_msg = self.router.output.recv() => {
                self.remote_sender.send(output_msg.into(), output, ctx).await
            }
        }
    }
}

pub(crate) struct DistributorBuilder<M: Kvt> {
    partition_func: WorkerPartitioner<M::Key>,
}

impl<M> DistributorBuilder<M>
where
    M: Kvt,
{
    pub(crate) fn new(partition_func: WorkerPartitioner<M::Key>) -> Self {
        Self { partition_func }
    }
}

impl<M> LogicBuilder<M, M> for DistributorBuilder<M>
where
    M: Kvt + Distributable,
    M::Key: Key + Distributable,
    M::Value: Distributable,
    M::Timestamp: Distributable,
{
    type Logic = Distributor<M>;

    async fn build(self, ctx: &mut BuildContext) -> Self::Logic {
        let remote_receiver = DistributorReceiver::new(ctx).await;
        let remote_sender = DistributorSender::new(ctx).await;
        let router = MessageRouter::spawn_new(ctx, self.partition_func);

        Distributor {
            remote_receiver,
            remote_sender,
            router,
        }
    }
}
