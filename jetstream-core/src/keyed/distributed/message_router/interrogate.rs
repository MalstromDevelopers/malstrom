use std::rc::Rc;

use indexmap::IndexSet;

use crate::{types::{Key, RescaleMessage, WorkerId}};
use super::super::types::*;
use super::{collect::CollectRouter, MessageRouter};


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
impl<K> InterrogateRouter<K> where K: Key {
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
                    self.queued_rescales);
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
