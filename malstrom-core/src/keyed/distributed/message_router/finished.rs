use indexmap::IndexSet;
use tracing::info;

use super::{super::types::*, MessageRouter, NormalRouter};
use crate::{channels::operator_io::Output, keyed::distributed::Remotes, types::*};
/// A collect route which has finished its local collection cycles
/// and is now just waiting for all remotes to finish
#[derive(Debug)]
pub(crate) struct FinishedRouter {
    pub(super) version: Version,
    old_worker_set: IndexSet<WorkerId>,
    pub(super) new_worker_set: IndexSet<WorkerId>,
    trigger: RescaleMessage,
}

impl FinishedRouter {
    pub(super) fn new(
        old_worker_set: IndexSet<WorkerId>,
        new_worker_set: IndexSet<WorkerId>,
        trigger: RescaleMessage,
    ) -> Self {
        Self {
            version: trigger.get_version(),
            old_worker_set,
            new_worker_set,
            trigger,
        }
    }

    pub(super) fn route_message<K, V, T>(
        &mut self,
        key: &K,
        partitioner: WorkerPartitioner<K>,
        this_worker: WorkerId,
        sender: WorkerId,
        remotes: &Remotes<K, V, T>,
    ) -> WorkerId {
        let new_target = partitioner(key, &self.new_worker_set);
        if new_target == this_worker {
            let old_target = partitioner(key, &self.old_worker_set);
            let old_target_version = remotes.get(&old_target).map(|x| x.1.last_version);
            info!(?new_target, ?old_target, ?old_target_version, ?self.version);
            if old_target == new_target {
                new_target
            }
            // TODO: I think we might not need this condition
            else if old_target == sender {
                this_worker
            // if the old target is already at our version, we know it has already
            // given up the state for this key (or did not have it in the first place)
            // and we can safely keep the message
            } else if remotes
                .get(&old_target)
                .unwrap()
                .1
                .last_version
                .map(|v| v == self.version)
                .unwrap_or(false)
            {
                new_target
            } else {
                old_target
            }
        } else {
            new_target
        }
    }

    pub(crate) fn lifecycle<K, V, T>(
        self,
        _partitioner: WorkerPartitioner<K>,
        output: &mut Output<K, V, T>,
        remotes: &mut Remotes<K, V, T>,
    ) -> MessageRouter<K, V, T>
    where
        K: Key,
        V: MaybeData,
        T: MaybeTime,
    {
        if remotes.values().all(|(_, state)| {
            state.last_version.map(|v| v == self.version).unwrap_or(false)
                // we cannot progress if they did not acknowledge our version because
                // in that case they might still try to send us messages
                && state.last_ack_version.map(|v| v == self.version).unwrap_or(false)
        }) {
            for wid in self.old_worker_set.difference(&self.new_worker_set) {
                remotes.swap_remove(wid);
            }
            remotes.shrink_to_fit();
            let normal_router = NormalRouter::new(self.new_worker_set, self.version);
            output.send(Message::Rescale(self.trigger));
            MessageRouter::Normal(normal_router)
        } else {
            MessageRouter::Finished(self)
        }
    }
}

#[cfg(test)]
mod tests {
    use indexmap::IndexSet;

    use crate::{
        channels::operator_io::{full_broadcast, link, Input, Output},
        keyed::{distributed::Remotes, partitioners::rendezvous_select},
        types::Message,
    };

    use super::{FinishedRouter, RescaleMessage};

    /// It should emit the original rescale message upon returning to the normal state
    #[test]
    fn emit_original_rescale() {
        let router = FinishedRouter::new(
            IndexSet::from([0]),
            IndexSet::from([0, 1]),
            RescaleMessage::new(IndexSet::from([1]), 0),
        );
        let mut output: Output<usize, usize, usize> = Output::new_unlinked(full_broadcast);
        let mut input = Input::new_unlinked();
        link(&mut output, &mut input);
        let mut remotes = Remotes::new();
        router.lifecycle(rendezvous_select, &mut output, &mut remotes);
        let msg = input.recv().unwrap();
        assert!(matches!(msg, Message::Rescale(_)))
    }
}
