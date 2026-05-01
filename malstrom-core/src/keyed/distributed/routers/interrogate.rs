use indexmap::IndexSet;

use crate::{
    channels::{recv_trait::Receiver as _, spsc},
    keyed::{
        WorkerPartitioner,
        distributed::{
            ConfigVersion, Interrogate, routers::{CollectRouter, NormalRouter, RouterInput, RouterKind, RouterOutput}, targeted_message::TargetedData, versioned_message::VersionedData
        },
    },
    types::{Key, Kvt, RescaleMessage, WorkerId, distributable::Distributable},
};

/// Router for the Interrogation phase of the ICA algorithm
pub(super) struct InterrogateRouter<M: Kvt> {
    pub this_version: ConfigVersion,
    pub this_worker: WorkerId,
    pub partition_func: WorkerPartitioner<M::Key>,
    /// We emit this once we are done reconfiguring for this key scope
    pub reconfig_message: RescaleMessage,
    pub old_worker_set: IndexSet<WorkerId>,
    /// interrogated key set arrives here
    interrogate_recv: tokio::sync::mpsc::UnboundedReceiver<M::Key>,
    /// interrogated keys
    keys: IndexSet<M::Key>,
}

impl<M> InterrogateRouter<M>
where
    M: Kvt,
    M::Key: Key + Distributable,
{
    pub(super) fn new(normal: NormalRouter<M>, trigger: RescaleMessage) -> (Self, Interrogate<M::Key>) {
        let (interrogate, interrogate_recv) = Interrogate::new();
        let this = Self {
            this_version: normal.this_version,
            this_worker: normal.this_worker,
            partition_func: normal.partition_func,
            reconfig_message: trigger,
            old_worker_set: normal.worker_set,
            interrogate_recv,
            keys: IndexSet::new(),
        };
        (this, interrogate)
    }

    pub(super) async fn apply(mut self,
        input: &mut spsc::Receiver<RouterInput<M>>,
        output: &mut spsc::Sender<RouterOutput<M>>,
    ) -> RouterKind<M> {
        tokio::select! {
            msg = input.recv() => {
                let msg = match msg {
                    RouterInput::DataMessage(versioned_data) => todo!(),
                    _ => unreachable!("Interrogate Router can only process data messages")
                };
                let targeted = self.route(msg);
                output.send(RouterOutput::DataMessage(targeted)).await;
                RouterKind::Interrogating(self)
            }
            key = self.interrogate_recv.recv() => {
                match key {
                    Some(k) => {self.keys.insert(k); RouterKind::Interrogating(self)},
                    // interrogation is done, all senders dropped
                    None => {
                        let router = CollectRouter::new(self);
                        RouterKind::Collecting(router)
                    }
                }
            }
        }
    }

    fn new_worker_set(&self) -> &IndexSet<WorkerId> {
        self.reconfig_message.get_all_workers()
    }

    fn route(&mut self, msg: VersionedData<M>) -> TargetedData<M> {
        let target_worker = if msg.config_version > self.this_version {
            self.this_worker
        } else {
            let key = &msg.data_msg.key;
            let old_target = (self.partition_func)(key, &self.old_worker_set);
            let new_target = (self.partition_func)(key, self.reconfig_message.get_all_workers());

            match (
                old_target == self.this_worker,
                new_target == self.this_worker,
            ) {
                // Rule 1.1.
                (true, false) => {
                    self.keys.insert(key.clone());
                    self.this_worker
                }
                // Rule 1.2
                (true, true) => self.this_worker,
                // Rule 2
                (false, _) => old_target,
            }
        };

        TargetedData {
            target_id: target_worker,
            config_version: self.this_version,
            data_msg: msg.data_msg,
        }
    }
}
