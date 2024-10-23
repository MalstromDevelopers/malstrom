use std::rc::Rc;

use indexmap::IndexSet;

use super::super::types::*;
use super::{collect::CollectRouter, MessageRouter};
use crate::types::{Key, RescaleMessage, WorkerId};

#[derive(Debug)]
pub(crate) struct InterrogateRouter<K> {
    pub(super) version: Version,
    // keys we will always pass on (for the moment)
    // pub(super) whitelist: IndexSet<K>,
    pub(super) old_worker_set: IndexSet<WorkerId>,
    pub(super) new_worker_set: IndexSet<WorkerId>,
    interrogate_msg: Interrogate<K>,

    // these are control messages we can not handle while rescaling
    // so we will buffer them, waiting for the normal dist to deal with them
    pub(super) queued_rescales: Vec<RescaleMessage>,
}
impl<K> InterrogateRouter<K>
where
    K: Key,
{
    pub(super) fn new(
        version: Version,
        old_worker_set: IndexSet<WorkerId>,
        trigger: RescaleMessage,
        partitioner: WorkerPartitioner<K>,
    ) -> (Self, Interrogate<K>)
    where
        K: Key,
    {
        let new_worker_set: IndexSet<WorkerId> = match trigger {
            RescaleMessage::ScaleRemoveWorker(to_remove) => {
                old_worker_set.difference(&to_remove).copied().collect()
            }
            RescaleMessage::ScaleAddWorker(to_add) => {
                old_worker_set.union(&to_add).copied().collect()
            }
        };

        let old_worker_set_clone = old_worker_set.clone();
        let new_worker_set_clone = new_worker_set.clone();

        // function telling us if a key needs to be moved from here
        // to a different worker under the new configuration
        let key_needs_to_be_moved = Rc::new(move |key: &K| {
            let original_target = partitioner(key, &old_worker_set_clone);
            let new_target = partitioner(key, &new_worker_set_clone);
            original_target != new_target
        });
        let interrogate_msg = Interrogate::new(key_needs_to_be_moved);

        let new_state = InterrogateRouter {
            version,
            old_worker_set,
            new_worker_set,
            interrogate_msg: interrogate_msg.clone(),
            queued_rescales: Vec::new(),
        };
        (new_state, interrogate_msg)
    }

    pub(super) fn route_message(
        &mut self,
        key: &K,
        partitioner: WorkerPartitioner<K>,
        this_worker: WorkerId,
    ) -> WorkerId {
        let old_target = partitioner(key, &self.old_worker_set);
        let new_target = partitioner(key, &self.new_worker_set);

        match (old_target == this_worker, new_target == this_worker) {
            // Rule 1.1.
            (true, false) => {
                self.interrogate_msg.add_keys(&[key.clone()]);
                this_worker
            }
            // Rule 1.2
            (true, true) => this_worker,
            // Rule 2
            (false, _) => old_target,
        }
    }

    pub(crate) fn lifecycle<V, T>(self) -> MessageRouter<K, V, T> {
        match self.interrogate_msg.try_unwrap() {
            Ok(whitelist) => {
                // interrogate is done
                let router = CollectRouter::new(
                    self.version,
                    whitelist,
                    self.old_worker_set,
                    self.new_worker_set,
                    self.queued_rescales,
                );
                MessageRouter::Collect(router)
            }
            // still running
            Err(e) => {
                let router = Self {
                    interrogate_msg: e,
                    ..self
                };
                MessageRouter::Interrogate(router)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::keyed::partitioners::index_select;

    /// It should create the new worker sets correctly on up and downscales
    #[test]
    fn create_new_worker_set() {
        // scale up
        let trigger = RescaleMessage::ScaleAddWorker(IndexSet::from([1, 2, 3]));
        let (router, _) = InterrogateRouter::new(0, IndexSet::from([0]), trigger, index_select);
        assert_eq!(router.new_worker_set, IndexSet::from([0, 1, 2, 3]));

        let trigger = RescaleMessage::ScaleRemoveWorker(IndexSet::from([2, 3]));
        let (router, _) =
            InterrogateRouter::new(0, IndexSet::from([0, 1, 2, 3]), trigger, index_select);
        assert_eq!(router.new_worker_set, IndexSet::from([0, 1]));
    }

    /// It should create an interrogate message
    /// which only accepts keys, that need redistribution
    #[test]
    fn creates_interrogate() {
        let trigger = RescaleMessage::ScaleAddWorker(IndexSet::from([1]));
        let (mut router, mut interrogate) =
            InterrogateRouter::new(0, IndexSet::from([0]), trigger, index_select);

        // this should add 1, 3
        interrogate.add_keys(&[0, 1, 2, 3, 4]);
        // should add 5
        router.route_message(&5, index_select, 0);
        drop(interrogate);

        // should create a collector since we dropped
        let collect: MessageRouter<usize, i32, i32> = router.lifecycle();
        match collect {
            MessageRouter::Collect(c) => {
                assert_eq!(c.whitelist, IndexSet::from([1, 3, 5]))
            }
            _ => panic!(),
        }
    }

    /// Should not create a collect router while we are keeping a ref to the interrogate
    #[test]
    fn noop_if_interrogate_is_running() {
        let trigger = RescaleMessage::ScaleAddWorker(IndexSet::from([1]));
        let (router, interrogate) =
            InterrogateRouter::new(0, IndexSet::from([0]), trigger, index_select);

        let router: MessageRouter<usize, i32, i32> = router.lifecycle();
        let router = match router {
            MessageRouter::Interrogate(x) => x,
            _ => panic!(),
        };

        drop(interrogate);
        let collect: MessageRouter<usize, i32, i32> = router.lifecycle();
        assert!(matches!(collect, MessageRouter::Collect(_)));
    }

    #[test]
    /// Handle Rule 1.1
    /// • Rule 1.1: If (F(K) == Local) && (F'(K) != Local)
    /// • add the key K to the set whitelist
    /// • pass the message downstream
    fn handle_data_rule_1_1() {
        let trigger = RescaleMessage::ScaleAddWorker(IndexSet::from([1]));
        let (mut router, interrogate) =
            InterrogateRouter::new(0, IndexSet::from([0]), trigger, index_select);

        let target = router.route_message(&43, index_select, 0);
        assert_eq!(target, 0);

        drop(interrogate);
        let collect: MessageRouter<usize, i32, i32> = router.lifecycle();
        match collect {
            MessageRouter::Collect(c) => {
                assert!(c.whitelist.contains(&43))
            }
            _ => panic!(),
        }
    }

    #[test]
    /// Handle Rule 1.2
    /// • Rule 1.2: If (F(K) == Local) && (F'(K) == Local)
    /// • pass the message downstream
    fn handle_data_rule_1_2() {
        let trigger = RescaleMessage::ScaleAddWorker(IndexSet::from([1]));
        let (mut router, _interrogate) =
            InterrogateRouter::new(0, IndexSet::from([0]), trigger, index_select);

        let target = router.route_message(&44, index_select, 0);
        assert_eq!(target, 0);
    }

    #[test]
    /// Handle Rule 2
    /// • Rule 2:  If (F(K) != Local)
    /// • send the message to the worker determined by F
    fn handle_data_rule_2() {
        let trigger = RescaleMessage::ScaleRemoveWorker(IndexSet::from([1]));
        let (mut router, _interrogate) =
            InterrogateRouter::new(0, IndexSet::from([0, 1]), trigger, index_select);

        let target = router.route_message(&11, index_select, 0);
        assert_eq!(target, 1)
    }
}
