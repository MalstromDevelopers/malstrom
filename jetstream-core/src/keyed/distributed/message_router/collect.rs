
use indexmap::IndexSet;

use crate::{
    channels::selective_broadcast::Sender,
    keyed::distributed::{NetworkDataMessage, Remotes},
    runtime::communication::broadcast,
    types::*,
};

use super::{
    finished::FinishedRouter, MessageRouter, NetworkMessage
};
use super::super::types::*;

pub(crate) struct CollectRouter<K, V, T> {
    pub(super) version: Version,

    whitelist: IndexSet<K>,
    old_worker_set: IndexSet<WorkerId>,
    new_worker_set: IndexSet<WorkerId>,
    /// Datamessages for the key we are currently collecting
    buffered: Vec<DataMessage<K, V, T>>,

    current_collect: Option<Collect<K>>,

    // these are control messages we can not handle while rescaling
    // so we will buffer them, waiting for the normal dist to deal with them
    pub(super) queued_rescales: Vec<RescaleMessage>,
}


impl<K, V, T> CollectRouter<K, V, T> where K: Key {
    pub(super) fn new(
        version: Version,
        whitelist: IndexSet<K>,
        old_worker_set: IndexSet<WorkerId>,
        new_worker_set: IndexSet<WorkerId>,
        queued_rescales: Vec<RescaleMessage>,
    ) -> Self {
        Self {
            version: version + 1,
            whitelist,
            old_worker_set,
            new_worker_set,
            buffered: Vec::new(),
            current_collect: None,
            queued_rescales,
        }
    }

    pub(super) fn route_message(
        &mut self,
        msg: DataMessage<K, V, T>,
        partitioner: WorkerPartitioner<K>,
        this_worker: WorkerId,
        sender: WorkerId,
    ) -> Option<(DataMessage<K, V, T>, WorkerId)> {
        let key = &msg.key;
        let new_target = partitioner(key, &self.new_worker_set);

        let in_whitelist = self.whitelist.contains(key);
        let to_be_buffered = self
            .current_collect.as_ref()
            .map_or(false, |collect| &collect.key == key);

        match (new_target == this_worker, in_whitelist, to_be_buffered) {
            // Rule 1.1, non-local && in_whitelist
            (false, true, _) => Some((msg, this_worker)),
            // Rule 1.2
            (false, false, true) => {
                self.buffered.push(msg);
                None
            }
            // Rule 2
            (false, false, false) => Some((msg, new_target)),
            // Rule 3
            (true, _, _) => {
                let old_target = partitioner(key, &self.old_worker_set);
                // TODO: memoize these keys
                if old_target == sender {
                    Some((msg, this_worker))
                } else {
                    Some((msg, old_target))
                }
            }
        }
    }
}

impl<K, V, T> CollectRouter<K, V, T>
where
    K: DistKey,
    V: DistData,
    T: DistTimestamp
{

    pub(crate) fn lifecycle(
        mut self,
        partitioner: WorkerPartitioner<K>,
        output: &mut Sender<K, V, T>,
        remotes: &Remotes<K, V, T>,
    ) -> MessageRouter<K, V, T> {

        // try finishing the state collection
        match self.current_collect.take().map(NetworkAcquire::try_from) {
            Some(Ok(acquire)) => {
                let target = partitioner(&acquire.key, &self.new_worker_set);
                let target_client = &remotes
                    .get(&target)
                    .expect("partitioner returns valid target")
                    .0;
                target_client.send(NetworkMessage::Acquire(acquire));
                for buffered_msg in self.buffered.drain(..) {
                    let net_msg = NetworkDataMessage::new(buffered_msg, self.version);
                    target_client.send(NetworkMessage::Data(net_msg));
                }
                self.set_and_emit_collect(output);
            }
            Some(Err(collect)) => {
                self.current_collect = Some(collect);
            }
            None => self.set_and_emit_collect(output),
        }

        if self.current_collect.is_none() && self.whitelist.is_empty() {
            broadcast(remotes.values().map(|(client, _)| client), NetworkMessage::Upgrade(self.version));
            MessageRouter::Finished(FinishedRouter::new(self.version, self.old_worker_set, self.new_worker_set, self.queued_rescales))
        } else {
            MessageRouter::Collect(self)
        }
    }

    fn set_and_emit_collect(&mut self, output: &mut Sender<K, V, T>) {
        if self.current_collect.is_none() {
            self.current_collect = self.whitelist.pop().map(Collect::new).map(|collect| {
                output.send(Message::Collect(collect.clone()));
                collect
            });
        }
    }

}
