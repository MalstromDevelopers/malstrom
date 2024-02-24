use std::{rc::Rc, sync::Mutex};

use indexmap::IndexSet;

use crate::{
    channels::selective_broadcast::Sender, keyed::WorkerPartitioner,
    stream::operator::OperatorContext, DataMessage, Message, WorkerId,
};

use super::{
    collect::CollectDistributor, DistData, DistKey, Interrogate, NetworkMessage, PhaseDistributor,
    ScalableMessage, Version, VersionedMessage,
};

pub(super) struct InterrogateDistributor<K, T> {
    whitelist: Rc<Mutex<IndexSet<K>>>,
    old_worker_set: IndexSet<WorkerId>,
    new_worker_set: IndexSet<WorkerId>,
    version: Version,
    finished: IndexSet<WorkerId>,
    // if we receive another scale instruction during the interrogation,
    // there is no real good way for us to handle that, so we will
    // queue it up to be handled after collect is done
    queued_rescales: Vec<ScalableMessage<K, T>>,
    running_interrogate: Interrogate<K>,
}
impl<K, T> InterrogateDistributor<K, T>
where
    K: DistKey,
    T: DistData,
{
    pub(super) fn new<P: Clone>(
        old_worker_set: IndexSet<WorkerId>,
        new_worker_set: IndexSet<WorkerId>,
        version: Version,
        finished: IndexSet<WorkerId>,
        output: &mut Sender<K, T, P>,
    ) -> Self {
        let whitelist = Rc::new(Mutex::new(IndexSet::new()));
        let interrogate = Interrogate { shared: whitelist };
        output.send(Message::Interrogate(interrogate.clone()));

        Self {
            whitelist: Rc::new(Mutex::new(IndexSet::new())),
            old_worker_set,
            new_worker_set,
            version,
            finished,
            queued_rescales: Vec::new(),
            running_interrogate: interrogate,
        }
    }
    fn send_local<P: Clone>(
        &self,
        dist_func: &impl WorkerPartitioner<K>,
        msg: DataMessage<K, T>,
        output: &mut Sender<K, T, P>,
        local_wid: WorkerId,
    ) {
        if *(dist_func)(&msg.key, &self.new_worker_set) != local_wid {
            self.whitelist.lock().unwrap().insert(msg.key.clone());
        }
        output.send(Message::Data(msg))
    }

    fn send_remote(&self, target: &WorkerId, msg: DataMessage<K, T>, ctx: &mut OperatorContext) {
        ctx.communication
            .send(
                target,
                NetworkMessage::Data(VersionedMessage {
                    sender: ctx.worker_id,
                    version: Some(self.version),
                    message: msg,
                }),
            )
            .expect("Network Send Error");
    }

    pub(super) fn run<P: Clone>(
        mut self,
        dist_func: &impl WorkerPartitioner<K>,
        msg: Option<ScalableMessage<K, T>>,
        output: &mut Sender<K, T, P>,
        ctx: &mut OperatorContext,
    ) -> PhaseDistributor<K, T> {
        match msg {
            Some(ScalableMessage::Data(d)) => {
                // if the messages version is greater than our version we send it
                // downstream
                if d.version.map_or(false, |v| v > self.version) {
                    self.send_local(dist_func, d.message, output, ctx.worker_id);
                    return PhaseDistributor::Interrogate(self);
                }
                let old_target = dist_func(&d.message.key, &self.old_worker_set);
                if *old_target != ctx.worker_id {
                    self.send_remote(old_target, d.message, ctx);
                } else {
                    self.send_local(dist_func, d.message, output, ctx.worker_id);
                }
            }
            Some(ScalableMessage::ScaleRemoveWorker(x)) => {
                self.queued_rescales
                    .push(ScalableMessage::ScaleRemoveWorker(x));
            }
            Some(ScalableMessage::ScaleAddWorker(x)) => {
                self.queued_rescales
                    .push(ScalableMessage::ScaleAddWorker(x));
            }
            Some(ScalableMessage::Done(x)) => {
                self.finished.insert(x);
            }
            None => (),
        };
        if self.running_interrogate.ref_count() == 1 {
            // SAFETY: We can take from the Rc since we just asserted, that we have the only
            // reference
            let whitelist = unsafe {
                Rc::try_unwrap(self.whitelist)
                    .unwrap_unchecked()
                    .into_inner()
                    .unwrap()
            };
            PhaseDistributor::Collect(CollectDistributor::new(
                whitelist,
                self.old_worker_set,
                self.new_worker_set,
                self.version,
                self.finished,
                self.queued_rescales,
            ))
        } else {
            PhaseDistributor::Interrogate(self)
        }
    }
}
