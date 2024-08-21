use std::{marker::PhantomData, rc::Rc};

use indexmap::IndexSet;
use serde::{Deserialize, Serialize};

use crate::{
    channels::selective_broadcast::{Receiver, Sender},
    keyed::WorkerPartitioner,
    snapshot::{Barrier, PersistenceClient},
    stream::operator::OperatorContext,
    time::MaybeTime,
    DataMessage, MaybeData, Message, WorkerId,
};

use super::{
    icadd_operator::{DistributorKind, TargetedMessage},
    interrogate_dist::InterrogateDistributor,
    versioner::VersionedMessage,
    DistKey, Version,
};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub(super) struct NormalDistributor<K, V, T> {
    pub(super) worker_id: WorkerId,
    pub(super) worker_set: IndexSet<WorkerId>,
    pub(super) version: Version,
    phantom: PhantomData<(K, V, T)>,
}
impl<K, V, T> NormalDistributor<K, V, T>
where
    K: DistKey,
    V: MaybeData,
    T: MaybeTime,
{
    pub(super) fn new(worker_id: WorkerId, worker_set: IndexSet<WorkerId>) -> Self {
        Self {
            worker_id,
            worker_set,
            version: 0,
            phantom: PhantomData,
        }
    }

    pub(super) fn new_from_collect(
        worker_id: WorkerId,
        worker_set: IndexSet<WorkerId>,
        barrier: Option<Barrier>,
        output: &mut Sender<K, TargetedMessage<V>, T>,
        ctx: &OperatorContext,
        partitioner: Rc<dyn WorkerPartitioner<K>>,
    ) -> DistributorKind<K, V, T> {
        let this = Self::new(worker_id, worker_set);

        if let Some(b) = barrier {
            return this.handle_msg(Message::AbsBarrier(b), output, ctx, partitioner);
        }
        DistributorKind::Normal(this)
    }

    fn handle_msg(
        self,
        msg: Message<K, VersionedMessage<V>, T>,
        output: &mut Sender<K, TargetedMessage<V>, T>,
        ctx: &OperatorContext,
        partitioner: Rc<dyn WorkerPartitioner<K>>,
    ) -> DistributorKind<K, V, T> {
        match msg {
            Message::Rescale(r) => {
                output.send(Message::Rescale(r.clone()));
                println!("Creating Interrogate Worker");
                return DistributorKind::Interrogate(InterrogateDistributor::new(
                    self.worker_id,
                    self.worker_set,
                    self.version,
                    r,
                ));
            }
            Message::Data(d) => {
                if d.value.version > self.version {
                    output.send(Message::Data(DataMessage::new(
                        d.key,
                        TargetedMessage {
                            inner: d.value,
                            target: None,
                        },
                        d.timestamp,
                    )));
                } else {
                    let target = partitioner(&d.key, &self.worker_set);
                    let vmsg = VersionedMessage {
                        inner: d.value.inner,
                        version: self.version,
                        sender: self.worker_id,
                    };
                    output.send(Message::Data(DataMessage::new(
                        d.key,
                        TargetedMessage {
                            inner: vmsg,
                            target: Some(*target),
                        },
                        d.timestamp,
                    )));
                }
            }
            Message::Epoch(e) => output.send(Message::Epoch(e)),
            Message::AbsBarrier(mut b) => {
                b.persist(&self, &ctx.operator_id);
                output.send(Message::AbsBarrier(b))
            }
            Message::ShutdownMarker(s) => {
                output.send(Message::ShutdownMarker(s));
            }
            Message::Interrogate(_) => unreachable!(),
            Message::Collect(_) => unreachable!(),
            Message::Acquire(_) => unreachable!(),
            Message::DropKey(_) => unreachable!(),
        }
        DistributorKind::Normal(self)
    }

    pub(super) fn run(
        self,
        input: &mut Receiver<K, VersionedMessage<V>, T>,
        output: &mut Sender<K, TargetedMessage<V>, T>,
        ctx: &OperatorContext,
        partitioner: Rc<dyn WorkerPartitioner<K>>,
    ) -> DistributorKind<K, V, T> {
        if let Some(msg) = input.recv() {
            self.handle_msg(msg, output, ctx, partitioner)
        } else {
            DistributorKind::Normal(self)
        }
    }
}

// #[cfg(test)]
// mod test {
//     use anyhow::Result;

//     use crate::{
//         keyed::distributed::messages::{
//             DoneMessage, LocalOutgoingMessage, RemoteMessage, VersionedMessage,
//         },
//         snapshot::{Barrier, NoPersistence},
//         test::CapturingPersistenceBackend,
//         time::NoTime,
//         DataMessage, NoData, NoKey,
//     };

//     use super::*;

//     // a partitioner that just uses the key as a wrapping index
//     fn partiton_index<'a>(i: &usize, s: &'a IndexSet<WorkerId>) -> &'a WorkerId {
//         s.get_index(i % s.len()).unwrap()
//     }

//     #[test]
//     /// Check data is forwared locally, if the dist_func says so
//     fn forward_data_locally() {
//         let mut dist = NormalDistributor::new(0, IndexSet::from([0]));

//         let input = DataMessage {
//             key: 0,
//             value: 42,
//             timestamp: NoTime,
//         };
//         let input2 = DataMessage {
//             key: 0,
//             value: 42,
//             timestamp: NoTime,
//         };

//         let result = dist.handle_msg(input, None, &partiton_index);
//         assert!(matches!(
//             result,
//             OutgoingMessage::Local(LocalOutgoingMessage::Data(DataMessage {
//                 key: 0,
//                 value: 42,
//                 timestamp: NoTime
//             }))
//         ));

//         let result = dist.handle_msg(input2, None, &partiton_index);
//         assert!(matches!(
//             result,
//             OutgoingMessage::Local(LocalOutgoingMessage::Data(DataMessage {
//                 key: 0,
//                 value: 42,
//                 timestamp: NoTime
//             }))
//         ));
//     }

//     #[test]
//     /// Check data is sent remotely, if the dist func says so
//     fn forward_data_remote() {
//         let mut dist = NormalDistributor::new(0, IndexSet::from([0, 1]));

//         let input = DataMessage {
//             key: 1,
//             value: 42,
//             timestamp: NoTime,
//         };
//         let result = dist.handle_msg(input, None, &partiton_index);

//         assert!(matches!(
//             result,
//             OutgoingMessage::Remote(1, VersionedMessage {
//                 version: 0,
//                 message: DataMessage {
//                     key: 1,
//                     value: 42,
//                     timestamp: NoTime
//                 }
//             })
//         ));
//     }

//     #[test]
//     /// Check data is ALWAYS forwarded locally, if the version is higher
//     fn forward_local_higher_version() {
//         let dist = NormalDistributor::new(0, IndexSet::from([0, 1]));

//         let input = DataMessage {
//             key: 1,
//             value: 42,
//             timestamp: NoTime,
//         };
//         let result = dist.handle_msg(input, Some(1), &partiton_index);
//         assert!(matches!(
//             result,
//             OutgoingMessage::Local(LocalOutgoingMessage::Data(DataMessage {
//                 key: 1,
//                 value: 42,
//                 timestamp: NoTime
//             }))
//         ));
//     }
// }
