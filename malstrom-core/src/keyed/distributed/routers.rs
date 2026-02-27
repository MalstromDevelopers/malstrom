use indexmap::IndexSet;
use serde::{Serialize, de::DeserializeOwned};
use std::hash::Hash;

use crate::{
    keyed::distributed::{NetworkDataMessage, Remotes},
    types::*,
};

use super::types::*;


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
        current_collect: Option<&Collect<M::Key>>,
    ) -> Option<WorkerId> {
        let new_target = (self.partitioner)(key, &self.new_worker_set);
        let in_whitelist = self.whitelist.contains(key);
        let to_be_buffered = current_collect
            .as_ref()
            .is_some_and(|collect| &collect.key == key);

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
        version: Version,
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


#[cfg(test)]
mod tests {
    use super::*;
    use indexmap::IndexSet;

    /// a partitioner that just uses the key as a wrapping index
    fn partition_index<'a>(i: &usize, s: &'a IndexSet<WorkerId>) -> WorkerId {
        *s.get_index(i % s.len()).unwrap()
    }

    #[test]
    fn test_collect_route_message_rule_1_1() {
        let key = 15;
        let whitelist = IndexSet::from([key]);
        let old_worker_set = IndexSet::from([0]);
        let new_worker_set = IndexSet::from([0, 1]);

        let router = CollectRouter::<(usize, i32, usize)>::new(
            0,
            whitelist,
            old_worker_set,
            new_worker_set,
            partition_index,
        );

        let result = router.route_message(&key, 0, None);

        // Rule 1.1: (F'(K) != Local) && K ∈ whitelist → should return this_worker (0)
        assert_eq!(result, Some(0));
    }

    #[test]
    fn test_collect_route_message_rule_1_2() {
        let key = 15;
        let whitelist = IndexSet::from([key]);
        let old_worker_set = IndexSet::from([0]);
        let new_worker_set = IndexSet::from([0, 1]);
        let current_collect = Some(Collect::new(key));

        let router = CollectRouter::<(usize, i32, usize)>::new(
            0,
            whitelist,
            old_worker_set,
            new_worker_set,
            partition_index,
        );

        let result = router.route_message(&key, 0, current_collect.as_ref());

        // Rule 1.2: (F'(K) != Local) && K ∈ hold → should buffer (return None)
        assert_eq!(result, None);
    }

    #[test]
    fn test_collect_route_message_rule_2() {
        let key = 7;
        let whitelist = IndexSet::from([3]);
        let old_worker_set = IndexSet::from([0]);
        let new_worker_set = IndexSet::from([0, 1]);

        let router = CollectRouter::<(usize, i32, usize)>::new(
            0,
            whitelist,
            old_worker_set,
            new_worker_set,
            partition_index,
        );

        let result = router.route_message(&key, 0, None);

        // Rule 2: (F'(K) != Local) && K ∉ whitelist && K ∉ hold → should return new_target (1)
        assert_eq!(result, Some(1));
    }

    #[test]
    fn test_collect_route_message_rule_3() {
        let key = 2;
        let whitelist = IndexSet::from([3]);
        let old_worker_set = IndexSet::from([0, 1]);
        let new_worker_set = IndexSet::from([0]);

        let router = CollectRouter::<(usize, i32, usize)>::new(
            0,
            whitelist,
            old_worker_set,
            new_worker_set,
            partition_index,
        );

        // Test case where old_target == sender
        let result = router.route_message(&key, 0, None);
        assert_eq!(result, Some(0));

        // Test case where old_target != sender
        let key2 = 3;
        let result = router.route_message(&key2, 0, None);
        assert_eq!(result, Some(1));
    }

    #[test]
    fn test_finished_route_message() {
        let key = 5;
        let old_worker_set = IndexSet::from([0, 1]);
        let new_worker_set = IndexSet::from([0]);
        let version = 1;

        // Create a mock remotes with old_target at our version
        let mut remotes: Remotes<(usize, i32, usize)> = Remotes::new();
        remotes.insert(
            1,
            (
                crate::runtime::CommunicationClient::fake(),
                crate::keyed::distributed::RemoteState {
                    last_version: Some(version),
                    last_ack_version: Some(version),
                },
            ),
        );

        let result = finished_route_message::<(usize, i32, usize)>(
            &key,
            partition_index,
            0,
            0,
            &old_worker_set,
            &new_worker_set,
            version,
            &remotes,
        );

        // Should return new_target (0) since old_target is at our version
        assert_eq!(result, 0);
    }

    #[test]
    fn test_interrogate_route_message_rule_1_1() {
        let key = 43;
        let old_worker_set = IndexSet::from([0]);
        let new_worker_set = IndexSet::from([0, 1]);

        let result =
            interrogate_route_message(&key, partition_index, 0, &old_worker_set, &new_worker_set);

        // Rule 1.1: (F(K) == Local) && (F'(K) != Local) → should return this_worker
        assert_eq!(result, 0);
    }

    #[test]
    fn test_interrogate_route_message_rule_1_2() {
        let key = 44;
        let old_worker_set = IndexSet::from([0]);
        let new_worker_set = IndexSet::from([0]);

        let result =
            interrogate_route_message(&key, partition_index, 0, &old_worker_set, &new_worker_set);

        // Rule 1.2: (F(K) == Local) && (F'(K) == Local) → should return this_worker
        assert_eq!(result, 0);
    }

    #[test]
    fn test_interrogate_route_message_rule_2() {
        let key = 11;
        let old_worker_set = IndexSet::from([0, 1]);
        let new_worker_set = IndexSet::from([0]);

        let result =
            interrogate_route_message(&key, partition_index, 0, &old_worker_set, &new_worker_set);

        // Rule 2: (F(K) != Local) → should return old_target (1)
        assert_eq!(result, 1);
    }

    #[test]
    fn test_normal_route_message() {
        let key = 5;
        let worker_set = IndexSet::from([0, 1]);

        let result = normal_route_message(&key, partition_index, &worker_set);

        // Should return target based on partitioner
        assert_eq!(result, 1);
    }
}
