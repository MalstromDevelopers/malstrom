use collect::CollectRouter;
use finished::FinishedRouter;
use indexmap::IndexSet;
use interrogate::InterrogateRouter;

use super::{types::*, Remotes};
use crate::{channels::operator_io::Output, types::*};

mod collect;
mod finished;
mod interrogate;
mod normal;

pub(super) use normal::NormalRouter;

#[derive(Debug)]
pub(super) enum MessageRouter<K, V, T> {
    Normal(NormalRouter),
    Interrogate(InterrogateRouter<K>),
    Collect(CollectRouter<K, V, T>),
    Finished(FinishedRouter),
}

impl<K, V, T> MessageRouter<K, V, T>
where
    K: Key,
{
    pub(super) fn new(worker_set: IndexSet<WorkerId>, version: Version) -> Self {
        let normal = NormalRouter::new(worker_set, version);
        Self::Normal(normal)
    }

    pub(super) fn route_message(
        &mut self,
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
                let target =
                    finished_state.route_message(&msg.key, partitioner, this_worker, sender);
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

impl<K, V, T> MessageRouter<K, V, T>
where
    K: DistKey,
    V: DistData,
    T: DistTimestamp,
{
    pub(super) fn handle_rescale(
        self,
        message: RescaleMessage,
        partitioner: WorkerPartitioner<K>,
        output: &mut Output<K, V, T>,
    ) -> MessageRouter<K, V, T> {
        match self {
            MessageRouter::Normal(normal_router) => {
                let (new_router, interrogate) = InterrogateRouter::new(
                    normal_router.version,
                    normal_router.worker_set,
                    message,
                    partitioner,
                );
                output.send(Message::Interrogate(interrogate));
                MessageRouter::Interrogate(new_router)
            }
            MessageRouter::Interrogate(_) => {
                unreachable!()
            }
            MessageRouter::Collect(_) => {
                unreachable!()
            }
            MessageRouter::Finished(_) => {
                unreachable!()
            }
        }
    }

    pub(super) fn lifecycle(
        self: MessageRouter<K, V, T>,
        partitioner: WorkerPartitioner<K>,
        output: &mut Output<K, V, T>,
        remotes: &mut Remotes<K, V, T>,
    ) -> MessageRouter<K, V, T> {
        match self {
            MessageRouter::Normal(normal_router) => MessageRouter::Normal(normal_router),
            MessageRouter::Interrogate(interrogate_router) => interrogate_router.lifecycle(),
            MessageRouter::Collect(collect_router) => {
                collect_router.lifecycle(partitioner, output, remotes)
            }
            MessageRouter::Finished(finished_router) => {
                finished_router.lifecycle(partitioner, output, remotes)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::keyed::partitioners::index_select;
    use proptest::prelude::*;

    proptest! {
    /// Check messages are always returned locally if they have a higher version
    #[test]
    fn higher_version(this_worker in 0u64..3, sender in 0u64..3, key in 0u64..100) {
        let mut normal_router = MessageRouter::Normal(NormalRouter::new(IndexSet::from([0, 1, 2]), 33));
        let mut interrogate_router = MessageRouter::Interrogate(InterrogateRouter::new(
            33,
            IndexSet::from([0, 1, 2]),
            RescaleMessage::new(IndexSet::from([3]), 1),
            index_select,
        ).0);
        let mut collect_router = MessageRouter::Collect(
            CollectRouter::new(IndexSet::from([]), IndexSet::from([0, 1, 2]), IndexSet::from([0, 1, 2, 3]

            ), RescaleMessage::new(IndexSet::from([4]), 33)));
        let mut finished_router = MessageRouter::Finished(
            FinishedRouter::new(
                IndexSet::from([0, 1, 2]),
                IndexSet::from([0, 1, 2, 3]),
                RescaleMessage::new(IndexSet::from([4]), 33))
            );


        let msg = DataMessage::new(key, 100, 0);

        let (msg, target) = normal_router.route_message(msg, Some(34), index_select, this_worker, sender).unwrap();
        assert_eq!(target, this_worker);

        let (msg, target) = interrogate_router.route_message(msg, Some(34), index_select, this_worker, sender).unwrap();
        assert_eq!(target, this_worker);

        // must increase the version here since the collect increases it on construction
        let (msg, target) = collect_router.route_message(msg, Some(35), index_select, this_worker, sender).unwrap();
        assert_eq!(target, this_worker);

        let (_msg, target) = finished_router.route_message(msg, Some(35), index_select, this_worker, sender).unwrap();
        assert_eq!(target, this_worker);
    }
    }
}
