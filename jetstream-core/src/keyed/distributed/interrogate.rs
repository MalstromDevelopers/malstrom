use std::{rc::Rc, sync::Mutex};

use indexmap::IndexSet;

use crate::{keyed::WorkerPartitioner, WorkerId};

use super::{dist_trait::{Distributor, DistributorKind}, messages::{IncomingMessage, Interrogate, OutgoingMessage, RescaleMessage}, normal::NormalDistributor, Version};

#[derive(Debug)]
pub(super) struct InterrogateDistributor<K> {
    pub(super) whitelist: Rc<Mutex<IndexSet<K>>>,
    pub(super) old_worker_set: IndexSet<WorkerId>,
    pub(super) new_worker_set: IndexSet<WorkerId>,
    pub(super) version: Version,
    pub(super) finished: IndexSet<WorkerId>,
    // if we receive another scale instruction during the interrogation,
    // there is no real good way for us to handle that, so we will
    // queue it up to be handled after collect is done
    pub(super) queued_rescales: Vec<RescaleMessage>,
    pub(super) running_interrogate: Option<Interrogate<K>>,
}

impl<K> InterrogateDistributor<K> {
    pub(super) fn from_normal(normal: NormalDistributor, trigger: RescaleMessage) -> Self {
        let old_worker_set = normal.worker_set;
        let new_worker_set: IndexSet<WorkerId> = match trigger {
            RescaleMessage::ScaleRemoveWorker(x) => old_worker_set.difference(&x).map(|y| y.clone()).collect(),
            RescaleMessage::ScaleAddWorker(x) => old_worker_set.union(&x).map(|y| y.clone()).collect(),
        };
        
        Self {
            whitelist: Rc::new(Mutex::new(IndexSet::new())),
            old_worker_set,
            new_worker_set,
            version: normal.version,
            finished: normal.finished,
            queued_rescales: Vec::new(),
            running_interrogate: None,
        }
    }
}

impl<K, V, T, P> Distributor<K, V, T, P> for InterrogateDistributor<K> {
    fn handle_msg(self, msg: IncomingMessage<K, V, T, P>, partitioner: impl WorkerPartitioner<K>) -> (DistributorKind<K, V, T>, OutgoingMessage<K, V, T, P>) {
        todo!()
    }

    fn run(self) -> (DistributorKind<K, V, T>, Option<OutgoingMessage<K, V, T, P>>) {
        todo!()
    }
}