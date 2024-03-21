use std::iter;

use indexmap::IndexSet;
use serde::{Deserialize, Serialize};

use crate::{
    keyed::WorkerPartitioner, snapshot::PersistenceBackend, stream::operator::BuildContext, DataMessage, WorkerId
};

use super::{
    dist_trait::{Distributor, DistributorKind},
    messages::{IncomingMessage, LocalOutgoingMessage, OutgoingMessage, RemoteMessage, TargetedMessage},
    Version,
};

#[derive(Serialize, Deserialize, Debug)]
pub(super) struct NormalDistributor {
    pub(super) worker_id: WorkerId,
    pub(super) worker_set: IndexSet<WorkerId>,

    pub(super) finished: IndexSet<WorkerId>,
    pub(super) version: Version,
}
impl NormalDistributor {
    pub(super) fn new(
        worker_id: WorkerId,
        peers: Vec<WorkerId>
    ) -> Self {
        let worker_set: IndexSet<WorkerId> = iter::once(worker_id.clone()).chain(peers.into_iter()).collect();
        Self { worker_id, worker_set, finished: IndexSet::new(), version: 0 }
    }

    /// Handle any data message reaching this operator
    fn handle_data<K, V, T, P>(&self, msg: DataMessage<K, V, T>, version: Option<Version>, partitioner: impl WorkerPartitioner<K>) -> OutgoingMessage<K, V, T, P> {
        if let Some(v) = version {
            if v > self.version {
                return OutgoingMessage::Local(LocalOutgoingMessage::Data(msg));
            }
        }
        let target = partitioner(&msg.key, &self.worker_set);
        if *target == self.worker_id {
            OutgoingMessage::Local(LocalOutgoingMessage::Data(msg))
        } else {
            let tm = TargetedMessage::new(*target, self.version, msg);
            OutgoingMessage::Remote(RemoteMessage::Data(tm))
        }
    }
}

impl<K, V, T, P> Distributor<K, V, T, P> for NormalDistributor {
    fn handle_msg(self, msg: IncomingMessage<K, V, T, P>, partitioner: impl WorkerPartitioner<K>) -> (DistributorKind<K, V, T>, OutgoingMessage<K, V, T, P>) {
        let mapped_msg = match msg {
            IncomingMessage::Data(d, v) => self.handle_data(d, v, partitioner),
            IncomingMessage::Epoch(_) => todo!(),
            IncomingMessage::AbsBarrier(_) => todo!(),
            IncomingMessage::ScaleRemoveWorker(_) => todo!(),
            IncomingMessage::ScaleAddWorker(_) => todo!(),
            IncomingMessage::ShutdownMarker(_) => todo!(),
            IncomingMessage::Done(_) => todo!(),
            IncomingMessage::Acquire(_) => todo!(),
        };
        (DistributorKind::Normal(self), mapped_msg)
    }

    fn run(self) -> (DistributorKind<K, V, T>, Option<OutgoingMessage<K, V, T, P>>) {
        todo!()
    }
}

#[cfg(test)]
mod test {
    use crate::{
        keyed::distributed::messages::{LocalOutgoingMessage, RemoteMessage, TargetedMessage}, snapshot::NoPersistence, time::NoTime, DataMessage
    };

    use super::*;

    // a partitioner that just uses the key as a wrapping index
    fn partiton_index<'a>(i: &usize, s: &'a IndexSet<WorkerId>) -> &'a WorkerId {
        s.get_index(i % s.len()).unwrap()
    }

    #[test]
    /// Check data is forwared locally, if the dist_func says so
    fn forward_data_locally() {
        let dist = NormalDistributor::new( 0, vec![]);

        let input: IncomingMessage<usize, i32, NoTime, NoPersistence> = IncomingMessage::Data(
            crate::DataMessage {
                key: 0,
                value: 42,
                timestamp: NoTime,
            },
            None,
        );
        let input2: IncomingMessage<usize, i32, NoTime, NoPersistence> = IncomingMessage::Data(
            crate::DataMessage {
                key: 0,
                value: 42,
                timestamp: NoTime,
            },
            Some(0),
        );

        let (new_dist, result) = dist.handle_msg(input, partiton_index);
        let dist = match new_dist {
            DistributorKind::Normal(x) => x,
            x => panic!("Wrong distributor returned {x:?}"),
        };
        assert!(matches!(
            result,
            OutgoingMessage::Local(LocalOutgoingMessage::Data(DataMessage {
                key: 0,
                value: 42,
                timestamp: NoTime
            }))
        ));

        let (new_dist, result) = dist.handle_msg(input2, partiton_index);
        let dist = match new_dist {
            DistributorKind::Normal(x) => x,
            x => panic!("Wrong distributor returned {x:?}"),
        };
        assert!(matches!(
            result,
            OutgoingMessage::Local(LocalOutgoingMessage::Data(DataMessage {
                key: 0,
                value: 42,
                timestamp: NoTime
            }))
        ));
    }

    #[test]
    /// Check data is sent remotely, if the dist func says so
    fn forward_data_remote() {
        let dist = NormalDistributor::new(0, vec![1]);

        let input: IncomingMessage<usize, i32, NoTime, NoPersistence> = IncomingMessage::Data(
            crate::DataMessage {
                key: 1,
                value: 42,
                timestamp: NoTime,
            },
            None,
        );
        let (new_dist, result) = dist.handle_msg(input, partiton_index);
        let dist = match new_dist {
            DistributorKind::Normal(x) => x,
            x => panic!("Wrong distributor returned {x:?}"),
        };

        assert!(matches!(
            result,
            OutgoingMessage::Remote(RemoteMessage::Data(TargetedMessage {
                target: 1,
                version: 0,
                message: DataMessage {
                    key: 1,
                    value: 42,
                    timestamp: NoTime
                }
            }))
        ));
    }

    #[test]
    /// Check data is ALWAYS forwarded locally, if the version is higher
    fn forward_local_higher_version() {
        let dist = NormalDistributor::new(0, vec![1]);

        let input: IncomingMessage<usize, i32, NoTime, NoPersistence> = IncomingMessage::Data(
            crate::DataMessage {
                key: 1,
                value: 42,
                timestamp: NoTime,
            },
            Some(1),
        );
        let (new_dist, result) = dist.handle_msg(input, partiton_index);
        let dist = match new_dist {
            DistributorKind::Normal(x) => x,
            x => panic!("Wrong distributor returned {x:?}"),
        };

        assert!(matches!(
            result,
            OutgoingMessage::Local(LocalOutgoingMessage::Data(DataMessage {
                key: 1,
                value: 42,
                timestamp: NoTime
            }))
        ));
    }
}
