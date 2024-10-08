use collect::CollectRouter;
use finished::FinishedRouter;
use indexmap::{IndexMap, IndexSet};
use interrogate::InterrogateRouter;

use crate::{channels::selective_broadcast::Sender, runtime::CommunicationClient, stream::OperatorContext, types::*};
use super::types::*;

mod collect;
mod finished;
mod interrogate;
mod normal;

pub(super) use normal::NormalRouter;

pub(super) enum MessageRouter<K, V, T> {
    Normal(NormalRouter),
    Interrogate(InterrogateRouter<K>),
    Collect(CollectRouter<K, V, T>),
    Finished(FinishedRouter)
}

impl<K, V, T> MessageRouter<K, V, T>
where
    K: Key,
{

    pub(super) fn new(worker_set: IndexSet<WorkerId>, version: Version) -> Self {
        let normal = NormalRouter::new(worker_set, version);
        Self::Normal(normal)
    }

    pub(super) fn route_message<'a>(
        &'a mut self,
        msg: DataMessage<K, V, T>,
        msg_version: Option<Version>,
        partitioner: WorkerPartitioner<K>,
        this_worker: WorkerId,
        sender: WorkerId,
    ) -> Option<(DataMessage<K, V, T>, WorkerId)> {
        if msg_version.map_or(false, |x| x > self.get_version()) {
            return Some((msg, this_worker));
        }
        match self {
            MessageRouter::Normal(normal_state) => {
                let target = normal_state.route_message(&msg.key, partitioner);
                Some((msg, target))
            }
            MessageRouter::Interrogate(interrogate_state) => {
                let target = interrogate_state.route_message(&msg.key, partitioner, this_worker);
                Some((msg, target))
            }
            MessageRouter::Collect(collect_state) => {
                collect_state.route_message(msg, partitioner, this_worker, sender)
            }
            MessageRouter::Finished(finished_state) => {
                let target = finished_state.route_message::<K, V, T>(&msg.key, partitioner, this_worker, sender);
                Some((msg, target))
            }
        }
    }

    pub(super) fn get_version(&self) -> Version {
        match self {
            MessageRouter::Normal(normal_state) => normal_state.version,
            MessageRouter::Interrogate(interrogate_state) => interrogate_state.version,
            MessageRouter::Collect(collect_state) => collect_state.version,
            MessageRouter::Finished(finished_router) => finished_router.version,
        }
    }
}

impl <K, V, T> MessageRouter<K, V, T> where K: DistKey, V: DistData, T: DistTimestamp {
        
    pub(super) fn handle_rescale(self, message: RescaleMessage, partitioner: WorkerPartitioner<K>, output: &mut Sender<K,V, T>) -> MessageRouter<K, V, T> {
        match self {
            MessageRouter::Normal(normal_router) => {
                let (new_router, interrogate) = InterrogateRouter::new(normal_router.version, normal_router.worker_set, message, partitioner);
                output.send(Message::Interrogate(interrogate));
                MessageRouter::Interrogate(new_router)
            },
            MessageRouter::Interrogate(mut interrogate_router) => {
                interrogate_router.queued_rescales.push(message);
                MessageRouter::Interrogate(interrogate_router)
            },
            MessageRouter::Collect(mut collect_router) => {
                collect_router.queued_rescales.push(message);
                MessageRouter::Collect(collect_router)
            }
            MessageRouter::Finished(mut finished_router) => {
                finished_router.queued_rescales.push(message);
                MessageRouter::Finished(finished_router)
            },
        }
    }

    pub(super) fn lifecycle(
        self: MessageRouter<K, V, T>,
        partitioner: WorkerPartitioner<K>,
        output: &mut Sender<K, V, T>,
        remotes: &IndexMap<
            WorkerId,
            (CommunicationClient<NetworkMessage<K, V, T>>, RemoteState<T>),
        >,
        ctx: &OperatorContext,
    ) -> MessageRouter<K, V, T> {
        match self {
            MessageRouter::Normal(normal_router) => MessageRouter::Normal(normal_router),
            MessageRouter::Interrogate(interrogate_router) => interrogate_router.lifecycle(),
            MessageRouter::Collect(collect_router) => {
                collect_router.lifecycle(partitioner, output, remotes, ctx)
            }
            MessageRouter::Finished(finished_router) => {
                finished_router.lifecycle(partitioner, output, remotes)
            }
        }
    }
}
