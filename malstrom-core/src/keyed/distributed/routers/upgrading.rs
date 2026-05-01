use indexmap::IndexSet;

use crate::{
    channels::{recv_trait::Receiver as _, spsc},
    keyed::{
        WorkerPartitioner,
        distributed::{
            ConfigVersion,
            routers::{CollectRouter, NormalRouter, RouterInput, RouterKind, RouterOutput},
            targeted_message::TargetedData,
            versioned_message::VersionedData,
        },
    },
    types::{Key, Kvt, RescaleMessage, WorkerId, distributable::Distributable},
};

pub(super) struct UpgradingRouter<M: Kvt> {
    this_version: ConfigVersion,
    this_worker: WorkerId,
    partition_func: WorkerPartitioner<M::Key>,
    old_worker_set: IndexSet<WorkerId>,
    new_worker_set: IndexSet<WorkerId>,
}

impl<M> UpgradingRouter<M>
where
    M: Kvt,
    M::Key: Key + Distributable,
{
    pub(super) fn new(collect_router: CollectRouter<M>) -> (Self, RescaleMessage) {
        let this = Self {
            // bump to next version
            this_version: collect_router.reconfig_message.get_version(),
            this_worker: collect_router.this_worker,
            partition_func: collect_router.partition_func,
            old_worker_set: collect_router.old_worker_set,
            new_worker_set: collect_router.reconfig_message.get_all_workers().clone(),
        };
        (this, collect_router.reconfig_message)
    }

    pub(super) async fn apply(
        mut self,
        input: &mut spsc::Receiver<RouterInput<M>>,
        output: &mut spsc::Sender<RouterOutput<M>>,
    ) -> RouterKind<M> {
        match input.recv().await {
            RouterInput::DataMessage(msg) => {
                let targeted = self.route(msg);
                output.send(RouterOutput::DataMessage(targeted)).await;
                RouterKind::Upgrading(self)
            }
            RouterInput::Complete(completion_msg) => {
                let router = NormalRouter::new(
                    self.this_version,
                    self.this_worker,
                    self.partition_func,
                    self.new_worker_set,
                );
                output.send(RouterOutput::Complete(completion_msg)).await;
                RouterKind::Normal(router)
            }
            _ => unreachable!("Concurrent reconfigs are not allowed"),
        }
    }

    fn route(&mut self, msg: VersionedData<M>) -> TargetedData<M> {
        if msg.config_version > self.this_version {
            return TargetedData {
                target_id: self.this_worker,
                config_version: msg.config_version,
                data_msg: msg.data_msg,
            };
        }

        let key = &msg.data_msg.key;
        let new_target = (self.partition_func)(key, &self.new_worker_set);
        let target_worker = if new_target != self.this_worker {
            new_target
        } else {
            let old_target = (self.partition_func)(key, &self.old_worker_set);
            if old_target == msg.sender_id {
                self.this_worker
            } else {
                old_target
            }
        };

        TargetedData {
            target_id: target_worker,
            config_version: self.this_version,
            data_msg: msg.data_msg,
        }
    }
}
