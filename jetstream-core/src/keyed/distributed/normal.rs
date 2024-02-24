use std::{marker::PhantomData, rc::Rc, sync::Mutex};

use bincode::config::Configuration;
use indexmap::{Equivalent, IndexMap, IndexSet};

use crate::{
    channels::selective_broadcast::{Receiver, Sender}, frontier::Timestamp, keyed::WorkerPartitioner, snapshot::PersistenceBackend, stream::operator::OperatorContext, Data, DataMessage, Key, Message, OperatorId, WorkerId
};

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use super::{interrogate::InterrogateDistributor, DistData, DistKey, NetworkMessage, PhaseDistributor, ScalableMessage, Version};

#[derive(Serialize, Deserialize, Default)]
pub(super) struct NormalDistributor {
    // includes local worker
    pub worker_set: IndexSet<WorkerId>,
    pub version: Version,
    pub finished: IndexSet<WorkerId>,
}
impl NormalDistributor {
    pub(super) fn run<K: DistKey, T: DistData, P: Clone>(
        mut self,
        dist_func: &impl WorkerPartitioner<K>,
        msg: Option<ScalableMessage<K, T>>,
        output: &mut Sender<K, T, P>,
        ctx: &mut OperatorContext,
    ) -> PhaseDistributor<K, T> {
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
                        .send(&target, NetworkMessage::Data(m))
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
                    old_set, new_set, self.version, self.finished, output,
                ))
            }
            ScalableMessage::ScaleAddWorker(set) => {
                let old_set = self.worker_set.clone();
                let new_set: IndexSet<WorkerId> =
                    self.worker_set.into_iter().chain(set.into_iter()).collect();
                PhaseDistributor::Interrogate(InterrogateDistributor::new(
                    old_set, new_set, self.version, self.finished, output,
                ))
            }
            ScalableMessage::Done(wid) => {
                self.finished.insert(wid);
                PhaseDistributor::Normal(self)
            }
        }
    }
}
