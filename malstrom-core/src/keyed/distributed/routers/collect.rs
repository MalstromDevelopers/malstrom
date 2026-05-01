use std::{collections::VecDeque, hash::Hash};

use indexmap::{IndexMap, IndexSet};
use tokio::sync::oneshot;

use crate::{
    channels::{operator_io::{Input, Output}, recv_trait::Receiver, spsc},
    keyed::{
        WorkerPartitioner,
        distributed::{
            Collect, ConfigVersion, Interrogate, remote_receiver::DistributorReceiver, remote_sender::DistributorSender, routers::{InterrogateRouter, RouterInput, RouterKind, RouterOutput, upgrading::UpgradingRouter}, targeted_message::TargetedData, versioned_message::{VersionedData, VersionedMessage}, wire_message::WireAcquire
        },
    },
    stream::{BuildContext, Logic, OperatorContext},
    types::{
        DataMessage, Key, Kvt, OperatorId, ReconfigComplete, RescaleMessage, WorkerId, distributable::Distributable
    },
};


pub(super) struct CollectRouter<M: Kvt> {
    pub this_version: ConfigVersion,
    pub this_worker: WorkerId,
    pub partition_func: WorkerPartitioner<M::Key>,

    /// We emit this once we are done reconfiguring for this key scope
    pub reconfig_message: RescaleMessage,
    pub old_worker_set: IndexSet<WorkerId>,
    /// keys we want to move, but which are currently still here
    whitelist: IndexSet<<M as Kvt>::Key>,
    /// Key and Receiver for currently running collection or None if currently no collect
    /// Operator states for the key we are currently collecting arrive here
    current_collect: Option<CollectState<M>>,
}
struct CollectState<M: Kvt> {
    /// The key being collected
    key: M::Key,
    /// collected states
    states: IndexMap<OperatorId, Vec<u8>>,
    /// Buffer for messages of the key we are currently collecting
    /// (Vec instead of VecDeque because we never pop)
    message_buffer: Vec<VersionedData<M>>,
    /// receiver to get states
    rx: tokio::sync::mpsc::UnboundedReceiver<(OperatorId, Vec<u8>)>
}

impl<M> CollectRouter<M>
where
    M: Kvt,
    M::Key: Key + Distributable,
{
    pub(super) fn new(interrogate: InterrogateRouter<M>) -> Self {
        /// TODO we could short circuit to [UpgradingRouter] here if no collectable keys
        Self {
            this_version: interrogate.this_version,
            this_worker: interrogate.this_worker,
            partition_func: interrogate.partition_func,
            reconfig_message: interrogate.reconfig_message,
            old_worker_set: interrogate.old_worker_set,
            whitelist: IndexSet::new(),
            current_collect: None,
        }
    }

    pub(super) async fn apply(mut self,
        input: &mut spsc::Receiver<RouterInput<M>>,
        output: &mut spsc::Sender<RouterOutput<M>>,
    ) -> RouterKind<M> {
        // this could probably be done more elegantly somehow, but meh
        let current_collect =  match self.current_collect.as_mut() {
            Some(x) => x,
            // there is no collect, try to create the next one, possibly ending the collection
            // phase if there are no keys left
            None => {match self.whitelist.pop() {
                Some(next_key) => {
                    let (collect, rx) = Collect::new(next_key.clone());
                    let collect_state = CollectState{
                        key: next_key,
                        states: IndexMap::new(),
                        message_buffer: Vec::new(),
                        rx
                    };
                    output.send(RouterOutput::Collect(collect)).await;
                    self.current_collect.insert(collect_state)
                }
                None => {
                    let (router, rescale_msg) = UpgradingRouter::new(self);
                    output.send(RouterOutput::Rescale(rescale_msg)).await;
                    return RouterKind::Upgrading(router);
                }
            }}
        };

        tokio::select!{
            // receive input msg
            msg = input.recv() => {
                let msg = match msg {
                    RouterInput::DataMessage(versioned_data) => versioned_data,
                    _ => unreachable!("Interrogate Router can only process data messages")
                };
                // buffer messages because key is currently getting collected
                if current_collect.key == msg.data_msg.key {
                    current_collect.message_buffer.push(msg)
                } else {
                    let targeted = self.route(msg);
                    output.send(RouterOutput::DataMessage(targeted)).await;
                }
                RouterKind::Collecting(self)
            }
            // state sent by operators
            state = current_collect.rx.recv() => {
                match state {
                    Some((operator_id, operator_state)) => {
                        // ignore previous entry. If an operator sends multiple states, that is
                        // weird, but fine with us, we just use the latest one
                        let _ = current_collect.states.insert(operator_id, operator_state);
                        RouterKind::Collecting(self)
                    },
                    // last instance of collect message dropped
                    None => {
                        // need to drop mut ref so we can take the Option
                        drop(current_collect);
                        // PANIC: We know it is Some because we just dropped the mut ref to it in
                        // the line above
                        let finished_collect = self.current_collect.take().expect("Must be Some");
                        // create acquire message
                        let acquire = WireAcquire::new(finished_collect.key, finished_collect.states);
                        output.send(RouterOutput::Acquire(acquire)).await;
                        // and emit all buffered messages
                        for msg in finished_collect.message_buffer.into_iter() {
                            let targeted = self.route(msg);
                            output.send(RouterOutput::DataMessage(targeted)).await
                        }
                        RouterKind::Collecting(self)
                    },
                }
            }
        }
    }

    /// Returns Option because we buffer messages for the key we are currently collecting
    fn route(&mut self, msg: VersionedData<M>) -> TargetedData<M> {
        let key = &msg.data_msg.key;
        if self.whitelist.contains(key) {
            let targeted = TargetedData {
                target_id: self.this_worker,
                config_version: self.this_version,
                data_msg: msg.data_msg,
            };
            return targeted;
        }

        // Oh god, what is this vomit below
        // TODO !!!!!
        let target = if msg.config_version > self.this_version {
            self.this_worker
        } else {
            let new_target = (self.partition_func)(key, &self.new_worker_set());
            if new_target == self.this_worker {
                let old_target = (self.partition_func)(key, &self.old_worker_set);
                if old_target == msg.sender_id {
                    self.this_worker
                } else {
                    old_target
                }
            } else {
                new_target
            }
        };

        let targeted = TargetedData {
            target_id: target,
            config_version: self.this_version,
            data_msg: msg.data_msg,
        };
        targeted
    }

    fn new_worker_set(&self) -> &IndexSet<WorkerId> {
        self.reconfig_message.get_all_workers()
    }
}