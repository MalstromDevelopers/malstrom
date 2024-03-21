use indexmap::{IndexMap, IndexSet};

use crate::{DataMessage, WorkerId};

use super::{
    dist_trait::Distributor, interrogate::InterrogateDistributor, messages::{Collect, RescaleMessage}, Version
};

#[derive(Debug)]
pub(super) struct CollectDistributor<K, V, T> {
    whitelist: IndexSet<K>,
    hold: IndexMap<K, Vec<DataMessage<K, V, T>>>,
    old_worker_set: IndexSet<WorkerId>,
    new_worker_set: IndexSet<WorkerId>,
    version: Version,
    finished: IndexSet<WorkerId>,
    // if we receive another scale instruction during the interrogation,
    // there is no real good way for us to handle that, so we will
    // queue it up to be handled after collect is done
    queued_rescales: Vec<RescaleMessage>,
    current_collect: Option<Collect<K>>,
}

impl<K, V, T> CollectDistributor<K, V, T> {
    pub(super) fn from_interrogate(interrogate: InterrogateDistributor<K>, whitelist: IndexSet<K>) -> Self {
        Self {
            whitelist: whitelist,
            hold: IndexMap::new(),
            old_worker_set: interrogate.old_worker_set,
            new_worker_set: interrogate.new_worker_set,
            version: interrogate.version,
            finished: interrogate.finished,
            queued_rescales: interrogate.queued_rescales,
            current_collect: None,
        }
    }
}

impl<K, V, T, P> Distributor<K, V, T, P> for CollectDistributor<K, V, T> {
    fn handle_msg(self, msg: super::messages::IncomingMessage<K, V, T, P>, partitioner: impl crate::keyed::WorkerPartitioner<K>) -> (super::dist_trait::DistributorKind<K, V, T>, super::messages::OutgoingMessage<K, V, T, P>) {
        todo!()
    }

    fn run(self) -> (super::dist_trait::DistributorKind<K, V, T>, Option<super::messages::OutgoingMessage<K, V, T, P>>) {
        todo!()
    }
}