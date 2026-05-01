use indexmap::IndexSet;

use crate::{
    channels::{recv_trait::Receiver as _, spsc},
    keyed::{
        WorkerPartitioner,
        distributed::{
            ConfigVersion,
            routers::{InterrogateRouter, RouterInput, RouterKind, RouterOutput},
            targeted_message::TargetedData,
            versioned_message::VersionedData,
        },
    },
    types::{Key, Kvt, WorkerId, distributable::Distributable},
};

/// A normal router which does not do anything but route messages to their target
pub(super) struct NormalRouter<M: Kvt> {
    pub this_version: ConfigVersion,
    pub this_worker: WorkerId,
    pub partition_func: WorkerPartitioner<M::Key>,
    pub worker_set: IndexSet<WorkerId>,
}

impl<M> NormalRouter<M>
where
    M: Kvt,
    M::Key: Key + Distributable,
{
    /// Create a new instance of [NormalRouter]
    pub(super) fn new(
        this_version: ConfigVersion,
        this_worker: WorkerId,
        partition_func: WorkerPartitioner<M::Key>,
        worker_set: IndexSet<WorkerId>,
    ) -> Self {
        Self {
            this_version,
            this_worker,
            partition_func,
            worker_set,
        }
    }

    pub(super) async fn apply(
        mut self,
        input: &mut spsc::Receiver<RouterInput<M>>,
        output: &mut spsc::Sender<RouterOutput<M>>,
    ) -> RouterKind<M> {
        let msg = input.recv().await;
        match msg {
            RouterInput::DataMessage(versioned_data) => {
                let targeted = self.route(versioned_data);
                output.send(RouterOutput::DataMessage(targeted)).await;
                RouterKind::Normal(self)
            }
            RouterInput::Rescale(rescale_message) => {
                let (router, interrogate_msg) = InterrogateRouter::new(self, rescale_message);
                output
                    .send(RouterOutput::Interrogate(interrogate_msg))
                    .await;
                RouterKind::Interrogating(router)
            }
            RouterInput::Complete(reconfig_complete) => todo!(),
        }
    }

    fn route(&mut self, msg: VersionedData<M>) -> TargetedData<M> {
        let msg_version = msg.sender_id;
        let target_worker = if msg_version > self.this_version {
            // pass on local
            self.this_worker
        } else {
            (self.partition_func)(&msg.data_msg.key, &self.worker_set)
        };
        TargetedData {
            target_id: target_worker,
            config_version: self.this_version,
            data_msg: msg.data_msg,
        }
    }
}
