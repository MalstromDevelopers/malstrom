use indexmap::IndexSet;

use super::{super::types::*, interrogate::InterrogateRouter, MessageRouter, NormalRouter};
use crate::{channels::selective_broadcast::Output, keyed::distributed::Remotes, types::*};
/// A collect route which has finished its local collection cycles
/// and is now just waiting for all remotes to finish
#[derive(Debug)]
pub(crate) struct FinishedRouter {
    pub(super) version: Version,

    old_worker_set: IndexSet<WorkerId>,
    new_worker_set: IndexSet<WorkerId>,

    // these are control messages we can not handle while rescaling
    // so we will buffer them, waiting for the normal dist to deal with them
    pub(super) queued_rescales: Vec<RescaleMessage>,
}

impl FinishedRouter {
    pub(super) fn new(
        version: Version,
        old_worker_set: IndexSet<WorkerId>,
        new_worker_set: IndexSet<WorkerId>,
        queued_rescales: Vec<RescaleMessage>,
    ) -> Self {
        Self {
            version,
            old_worker_set,
            new_worker_set,
            queued_rescales,
        }
    }

    pub(super) fn route_message<K>(
        &mut self,
        key: &K,
        partitioner: WorkerPartitioner<K>,
        this_worker: WorkerId,
        sender: WorkerId,
    ) -> WorkerId {
        let new_target = partitioner(key, &self.new_worker_set);

        if new_target == this_worker {
            let old_target = partitioner(key, &self.old_worker_set);
            // TODO: memoize these keys
            if old_target == sender {
                this_worker
            } else {
                old_target
            }
        } else {
            new_target
        }
    }

    pub(crate) fn lifecycle<K, V, T>(
        mut self,
        partitioner: WorkerPartitioner<K>,
        output: &mut Output<K, V, T>,
        remotes: &Remotes<K, V, T>,
    ) -> MessageRouter<K, V, T>
    where
        K: Key,
        V: MaybeData,
        T: MaybeTime,
    {
        // TODO: Remove removed remotes
        if remotes
            .values()
            .all(|(_, state)| state.last_version >= self.version)
        {
            match self.queued_rescales.pop() {
                Some(rescale) => {
                    let (interrogate_router, interrogate_msg) = InterrogateRouter::new(
                        self.version,
                        self.new_worker_set,
                        rescale,
                        partitioner,
                    );
                    output.send(Message::Interrogate(interrogate_msg));
                    MessageRouter::Interrogate(interrogate_router)
                }
                None => {
                    let normal_router = NormalRouter::new(self.new_worker_set, self.version);
                    MessageRouter::Normal(normal_router)
                }
            }
        } else {
            MessageRouter::Finished(self)
        }
    }
}
