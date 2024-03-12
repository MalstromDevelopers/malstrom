use std::{rc::Rc, sync::Mutex};

use indexmap::{Equivalent, IndexMap, IndexSet};

use crate::{
    channels::selective_broadcast::Sender, keyed::WorkerPartitioner,
    stream::operator::OperatorContext, DataMessage, Message, WorkerId,
};

use super::{
    normal::NormalDistributor, send_to_target, Collect, DistData, DistKey, DistTimestamp,
    NetworkAcquire, NetworkMessage, PhaseDistributor, ScalableMessage, Version, VersionedMessage,
};

pub(super) struct CollectDistributor<K, V, T> {
    whitelist: IndexSet<K>,
    hold: IndexMap<K, Vec<(V, T)>>,
    old_worker_set: IndexSet<WorkerId>,
    new_worker_set: IndexSet<WorkerId>,
    version: Version,
    finished: IndexSet<WorkerId>,
    // if we receive another scale instruction during the interrogation,
    // there is no real good way for us to handle that, so we will
    // queue it up to be handled after collect is done
    queued_rescales: Vec<ScalableMessage<K, V, T>>,
    current_collect: Option<Collect<K>>,
}
impl<K, V, T> CollectDistributor<K, V, T>
where
    K: DistKey,
    V: DistData,
    T: DistTimestamp,
{
    pub(super) fn run<P>(
        mut self,
        dist_func: &impl WorkerPartitioner<K>,
        msg: Option<ScalableMessage<K, V, T>>,
        output: &mut Sender<K, V, T, P>,
        ctx: &mut OperatorContext,
    ) -> PhaseDistributor<K, V, T> {
        match msg {
            Some(ScalableMessage::Data(m)) => {
                if m.version.map_or(false, |v| v > self.version) {
                    output.send(Message::Data(m.message));
                } else if let Some(e) = self.hold.get_mut(&m.message.key) {
                    // Rule 1.2
                    e.push((m.message.value, m.message.time))
                } else {
                    let new_target = *dist_func(&m.message.key, &self.new_worker_set);
                    let old_target = dist_func(&m.message.key, &self.old_worker_set);
                    // Rule 1.1
                    if (new_target != ctx.worker_id) && self.whitelist.contains(&m.message.key) {
                        output.send(Message::Data(m.message))
                    } else if new_target != ctx.worker_id {
                        // Rule 2
                        send_to_target(m, &new_target, output, ctx)
                    } else if new_target == m.sender {
                        // Rule 3
                        send_to_target(m, &ctx.worker_id, output, ctx)
                    } else {
                        // Rule 3
                        send_to_target(m, old_target, output, ctx)
                    }
                };
            }
            Some(ScalableMessage::ScaleRemoveWorker(set)) => {
                self.queued_rescales
                    .push(ScalableMessage::ScaleRemoveWorker(set));
            }
            Some(ScalableMessage::ScaleAddWorker(set)) => {
                self.queued_rescales
                    .push(ScalableMessage::ScaleAddWorker(set));
            }
            Some(ScalableMessage::Done(wid)) => {
                self.finished.insert(wid);
            }
            None => (),
        };
        match self.current_collect.take() {
            Some(cc) => {
                if cc.ref_count() > 1 {
                    self.current_collect = Some(cc);
                } else {
                    // collect is done
                    let key = cc.key;
                    let held_msgs = self.hold.swap_remove(&key);
                    let acquire_target = dist_func(&key, &self.new_worker_set);

                    // PANIC: We can unwrap here, as we already asserted, that we are
                    // the only reference to the RC in the if
                    let collection = Rc::try_unwrap(cc.collection).unwrap().into_inner().unwrap();
                    let acquire = NetworkAcquire {
                        key: key.clone(),
                        collection,
                    };
                    ctx.communication
                        .send(acquire_target, NetworkMessage::<K, V, T>::Acquire(acquire))
                        .expect("Communication error");

                    for msg in held_msgs.into_iter().flatten() {
                        ctx.communication
                            .send(
                                acquire_target,
                                NetworkMessage::Data(VersionedMessage {
                                    version: Some(self.version),
                                    sender: ctx.worker_id,
                                    message: DataMessage {
                                        time: msg.0,
                                        key: key.clone(),
                                        value: msg.1,
                                    },
                                }),
                            )
                            .expect("Communication error");
                    }

                    let _next_collect = self.whitelist.pop().map(|x| {
                        self.hold.insert(x.clone(), Vec::new());
                        Collect {
                            key: x,
                            collection: Rc::new(Mutex::new(IndexMap::new())),
                        }
                    });
                }
            }
            None => {
                if let Some(next_key) = self.whitelist.pop() {
                    self.hold.insert(next_key.clone(), Vec::new());
                    let collect = Collect::new(next_key);

                    self.current_collect = Some(collect.clone());
                    output.send(Message::Collect(collect))
                } else if !self.finished.contains(&ctx.worker_id) {
                    self.finished.insert(ctx.worker_id);
                    ctx.communication
                        .broadcast(NetworkMessage::<K, V, T>::Done(ctx.worker_id))
                        .expect("Network error")
                }
            }
        };
        if self.finished.equivalent(&self.old_worker_set) {
            let normal = NormalDistributor {
                worker_set: self.new_worker_set,
                version: self.version,
                finished: IndexSet::new(),
            };
            PhaseDistributor::Normal(normal)
        } else {
            PhaseDistributor::Collect(self)
        }
    }

    pub(super) fn new(
        whitelist: IndexSet<K>,
        old_worker_set: IndexSet<WorkerId>,
        new_worker_set: IndexSet<WorkerId>,
        version: Version,
        finished: IndexSet<WorkerId>,
        queued_rescales: Vec<ScalableMessage<K, V, T>>,
    ) -> Self {
        Self {
            whitelist,
            hold: IndexMap::new(),
            old_worker_set,
            new_worker_set,
            version: version + 1,
            finished,
            queued_rescales,
            current_collect: None,
        }
    }
}
