use std::{marker::PhantomData, rc::Rc};

use indexmap::IndexSet;
use serde::{Deserialize, Serialize};

use crate::{
    channels::selective_broadcast::{Receiver, Sender},
    keyed::types::{DistKey, WorkerPartitioner},
    snapshot::Barrier,
    stream::{Logic, OperatorContext},
    types::{DataMessage, MaybeData, Message, Timestamp, WorkerId},
};

use super::{
    icadd_operator::{DistributorKind, TargetedMessage},
    interrogate_dist::InterrogateDistributor,
    versioner::VersionedMessage,
    Version,
};

/// Normal case distributor, i.e. when there is no rescaling in progress
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
    T: Timestamp,
{
    /// create a new distributor
    pub(super) fn new(worker_id: WorkerId, worker_set: IndexSet<WorkerId>) -> Self {
        Self {
            worker_id,
            worker_set,
            version: 0,
            phantom: PhantomData,
        }
    }

    /// create a new one from a collect distributor, i.e. after we sucessfully rescaled
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

    /// handles an incoming message
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

#[cfg(test)]
mod test {
    use super::*;
    use crate::keyed::distributed::icadd;
    use crate::keyed::distributed::icadd_operator::make_icadd_with_dist;
    use crate::testing::*;
    use crate::types::*;

    /// a partitioner that just uses the key as a wrapping index
    fn partiton_index<'a>(i: &usize, s: &'a IndexSet<WorkerId>) -> &'a WorkerId {
        s.get_index(i % s.len()).unwrap()
    }

    fn get_tester(
    ) -> OperatorTester<usize, VersionedMessage<i32>, i32, usize, TargetedMessage<i32>, i32, ()>
    {
        OperatorTester::built_by(
            |ctx| {
                make_icadd_with_dist(
                    Rc::new(partiton_index),
                    DistributorKind::Normal(NormalDistributor::new(ctx.worker_id, ctx.get_worker_ids().into_iter().collect())),
                )
            },
            0,
            5,
            0..2,
        )
    }

    #[test]
    /// Check data is forwared locally, if the dist_func says so
    fn forward_data_locally() {
        let mut op = get_tester();

        let input1 = DataMessage {
            key: 0,
            value: VersionedMessage {
                inner: 42,
                version: 0,
                sender: 0,
            },
            timestamp: 0,
        };
        let input2 = DataMessage {
            key: 0,
            value: VersionedMessage {
                inner: 42,
                version: 0,
                sender: 1,
            },
            timestamp: 0,
        };

        op.send_local(Message::Data(input1));
        op.step();
        assert!(op.recv_local().is_some());

        op.send_local(Message::Data(input2));
        op.step();
        assert!(op.recv_local().is_some());
    }

    #[test]
    /// Check data is sent remotely, if the dist func says so
    fn forward_data_remote() {
        let mut op = get_tester();

        let input = DataMessage {
            key: 1,
            value: VersionedMessage {
                inner: 42,
                version: 0,
                sender: 0,
            },
            timestamp: 100,
        };
        op.send_local(Message::Data(input));
        op.step();
        let out = op.recv_local().unwrap();

        // target should be 1 -> non-local
        assert!(matches!(
            out,
            Message::Data(DataMessage {
                key: 1,
                value: TargetedMessage {
                    inner: VersionedMessage {
                        inner: 42,
                        version: 0,
                        sender: 0
                    },
                    target: Some(1)
                },
                timestamp: 100
            })
        ), "{out:?}");
    }

    #[test]
    /// Check data is ALWAYS forwarded locally, if the version is higher
    fn forward_local_higher_version() {
        let mut op = get_tester();

        let input = DataMessage {
            key: 0,
            value: VersionedMessage {
                inner: 42,
                version: 1234,
                sender: 1,
            },
            timestamp: 555,
        };
        op.send_local(Message::Data(input));
        op.step();
        let out = op.recv_local().unwrap();

        // target should be None -> local
        assert!(matches!(
            out,
            Message::Data(DataMessage {
                key: 0,
                value: TargetedMessage {
                    inner: VersionedMessage {
                        inner: 42,
                        version: 1234,
                        sender: 1
                    },
                    target: None
                },
                timestamp: 555
            })
        ), "{out:?}");
    }
}
