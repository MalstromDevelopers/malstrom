use indexmap::IndexSet;
use serde::{Serialize, de::DeserializeOwned};
use std::hash::Hash;

use crate::{
    keyed::distributed::{Remotes, wire_message::VersionedMessage},
    types::*,
};

use super::types::*;

pub(super) trait MessageRouter<K> {
    fn route_message(&mut self, key: &K, sender: WorkerId) -> WorkerId;
}


/// Router for normal message routing
/// Implements the same routing rules as in message_router/normal.rs
pub struct NormalRouter<K: Key> {
    worker_set: IndexSet<WorkerId>,
    partitioner: WorkerPartitioner<K>,
}

impl<K: Key> NormalRouter<K> {
    pub fn new(worker_set: IndexSet<WorkerId>, partitioner: WorkerPartitioner<K>) -> Self {
        Self {
            worker_set,
            partitioner,
        }
    }

    pub fn route_message(&self, key: &K) -> WorkerId {
        let target = (self.partitioner)(key, &self.worker_set);
        debug_assert!(self.worker_set.contains(&target));
        target
    }
}

/// Router for interrogate message routing
/// Implements the same routing rules as in message_router/interrogate.rs
pub struct InterrogateRouter<K: Key + Serialize + DeserializeOwned> {
    this_worker: WorkerId,
    old_worker_set: IndexSet<WorkerId>,
    new_worker_set: IndexSet<WorkerId>,
    partitioner: WorkerPartitioner<K>,
}

impl<K: Key + Serialize + DeserializeOwned> InterrogateRouter<K> {
    pub fn new(
        this_worker: WorkerId,
        old_worker_set: IndexSet<WorkerId>,
        new_worker_set: IndexSet<WorkerId>,
        partitioner: WorkerPartitioner<K>,
    ) -> Self {
        Self {
            this_worker,
            old_worker_set,
            new_worker_set,
            partitioner,
        }
    }

    pub fn route_message(&self, key: &K) -> WorkerId {
        let old_target = (self.partitioner)(key, &self.old_worker_set);
        let new_target = (self.partitioner)(key, &self.new_worker_set);

        match (old_target == self.this_worker, new_target == self.this_worker) {
            // Rule 1.1: (F(K) == Local) && (F'(K) != Local)
            // Key would be added to whitelist by caller
            (true, false) => self.this_worker,
            // Rule 1.2: (F(K) == Local) && (F'(K) == Local)
            (true, true) => self.this_worker,
            // Rule 2: (F(K) != Local)
            (false, _) => old_target,
        }
    }
}


/// Router for collect message routing
/// Implements the same routing rules as in message_router/collect.rs
pub struct CollectRouter<M: Kvt> {
    this_worker: WorkerId,
    whitelist: IndexSet<M::Key>,
    old_worker_set: IndexSet<WorkerId>,
    new_worker_set: IndexSet<WorkerId>,
    partitioner: WorkerPartitioner<M::Key>,
}

impl<M: Kvt> CollectRouter<M>
where
    M::Key: Key + Serialize + DeserializeOwned,
    M::Value: Serialize + DeserializeOwned,
    M::Timestamp: Serialize + DeserializeOwned,
{
    pub fn new(
        this_worker: WorkerId,
        whitelist: IndexSet<M::Key>,
        old_worker_set: IndexSet<WorkerId>,
        new_worker_set: IndexSet<WorkerId>,
        partitioner: WorkerPartitioner<M::Key>,
    ) -> Self {
        Self {
            this_worker,
            whitelist,
            old_worker_set,
            new_worker_set,
            partitioner,
        }
    }

    pub fn route_message(
        &self,
        key: &M::Key,
        sender: WorkerId,
    ) -> Option<WorkerId> {
        let new_target = (self.partitioner)(key, &self.new_worker_set);
        let in_whitelist = self.whitelist.contains(key);
        match (new_target == self.this_worker, in_whitelist, to_be_buffered) {
            // Rule 1.1, non-local && in_whitelist
            (false, true, _) => Some(self.this_worker),
            // Rule 1.2
            (false, false, true) => None, // Buffer the message
            // Rule 2
            (false, false, false) => Some(new_target),
            // Rule 3
            (true, _, _) => {
                let old_target = (self.partitioner)(key, &self.old_worker_set);
                if old_target == sender {
                    Some(self.this_worker)
                } else {
                    Some(old_target)
                }
            }
        }
    }
}

/// Router for finished message routing
/// Implements the same routing rules as in message_router/finished.rs
pub struct FinishedRouter<M: Kvt> {
    this_worker: WorkerId,
    old_worker_set: IndexSet<WorkerId>,
    new_worker_set: IndexSet<WorkerId>,
    partitioner: WorkerPartitioner<M::Key>,
}

impl<M: Kvt> FinishedRouter<M>
where
    M::Key: Key + Serialize + DeserializeOwned,
    M::Value: Serialize + DeserializeOwned,
    M::Timestamp: Serialize + DeserializeOwned,
{
    pub fn new(
        this_worker: WorkerId,
        old_worker_set: IndexSet<WorkerId>,
        new_worker_set: IndexSet<WorkerId>,
        partitioner: WorkerPartitioner<M::Key>,
    ) -> Self {
        Self {
            this_worker,
            old_worker_set,
            new_worker_set,
            partitioner,
        }
    }

    pub fn route_message(
        &self,
        key: &M::Key,
        sender: WorkerId,
        // version: Version,
        // remotes: &Remotes<M>, // TODO: instead use task like for interrogate/collect
    ) -> WorkerId {
        let new_target = (self.partitioner)(key, &self.new_worker_set);
        if new_target != self.this_worker {
            return new_target;
        }

        let old_target = (self.partitioner)(key, &self.old_worker_set);
        // let old_target_version = remotes.get(&old_target).map(|x| x.1.last_version);
        if old_target == new_target {
            return new_target;
        }
        if old_target == sender {
            return self.this_worker;
        }

        // if remotes
        //     .get(&old_target)
        //     .map(|v| Some(version) == v.1.last_version)
        //     .unwrap_or(false)
        // {
        //     return new_target;
        // }
        old_target
    }
}

