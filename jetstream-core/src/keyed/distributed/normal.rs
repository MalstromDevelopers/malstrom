use indexmap::IndexSet;

use crate::{
    channels::selective_broadcast::Sender, keyed::WorkerPartitioner,
    stream::operator::OperatorContext, Message, WorkerId,
};

use serde::{Deserialize, Serialize};

use super::{
    interrogate::InterrogateDistributor, DistData, DistKey, DistTimestamp, NetworkMessage,
    PhaseDistributor, ScalableMessage, Version,
};

#[derive(Serialize, Deserialize, Default)]
pub(super) struct NormalDistributor {
    // includes local worker
    pub worker_set: IndexSet<WorkerId>,
    pub version: Version,
    pub finished: IndexSet<WorkerId>,
}
impl NormalDistributor {
    pub(super) fn run<K: DistKey, V: DistData, T: DistTimestamp, P>(
        mut self,
        dist_func: &impl WorkerPartitioner<K>,
        msg: Option<ScalableMessage<K, V, T>>,
        output: &mut Sender<K, V, T, P>,
        ctx: &mut OperatorContext,
    ) -> PhaseDistributor<K, V, T> {
        // TODO HACK
        if self.worker_set.is_empty() {
            let mut wids: Vec<WorkerId> = vec![ctx.worker_id];
            wids.extend(ctx.communication.get_peers());
            wids.sort();
            self.worker_set.extend(wids);
        }

        let msg = match msg {
            Some(m) => m,
            None => {
                return PhaseDistributor::Normal(self);
            }
        };
        match msg {
            ScalableMessage::Data(m) => {
                if m.version.map_or(false, |v| v > self.version) {
                    output.send(Message::Data(m.message));
                    return PhaseDistributor::Normal(self);
                }
                let target = dist_func(&m.message.key, &self.worker_set);
                if *target == ctx.worker_id {
                    output.send(Message::Data(m.message));
                } else {
                    ctx.communication
                        .send(target, NetworkMessage::Data(m))
                        .expect("Remote send Error");
                };
                PhaseDistributor::Normal(self)
            }
            ScalableMessage::ScaleRemoveWorker(set) => {
                let old_set = self.worker_set.clone();
                let new_set: IndexSet<WorkerId> = self
                    .worker_set
                    .into_iter()
                    .filter(|x| !set.contains(x))
                    .collect();
                PhaseDistributor::Interrogate(InterrogateDistributor::new(
                    old_set,
                    new_set,
                    self.version,
                    self.finished,
                    output,
                ))
            }
            ScalableMessage::ScaleAddWorker(set) => {
                let old_set = self.worker_set.clone();
                let new_set: IndexSet<WorkerId> = self.worker_set.into_iter().chain(set).collect();
                PhaseDistributor::Interrogate(InterrogateDistributor::new(
                    old_set,
                    new_set,
                    self.version,
                    self.finished,
                    output,
                ))
            }
            ScalableMessage::Done(wid) => {
                self.finished.insert(wid);
                PhaseDistributor::Normal(self)
            }
        }
    }
}
