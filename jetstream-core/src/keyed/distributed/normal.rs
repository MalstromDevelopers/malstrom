use std::iter;

use indexmap::{IndexMap, IndexSet};
use serde::{Deserialize, Serialize};

use crate::{
    keyed::WorkerPartitioner, snapshot::PersistenceBackend, stream::operator::BuildContext,
    DataMessage, Key, WorkerId,
};

use super::{
    dist_trait::{Distributor, DistributorKind},
    interrogate::InterrogateDistributor,
    messages::{
        IncomingMessage, LocalOutgoingMessage, OutgoingMessage, RemoteMessage, TargetedMessage,
    },
    Version,
};

#[derive(Serialize, Deserialize, Debug)]
pub(super) struct NormalDistributor {
    pub(super) worker_id: WorkerId,
    pub(super) worker_set: IndexSet<WorkerId>,

    pub(super) remote_versions: IndexMap<WorkerId, Version>,
    pub(super) version: Version,
}
impl NormalDistributor {
    pub(super) fn new(worker_id: WorkerId, peers: Vec<WorkerId>) -> Self {
        let worker_set: IndexSet<WorkerId> = iter::once(worker_id.clone())
            .chain(peers.into_iter())
            .collect();
        let remote_versions = [(worker_id, 0)].into_iter().collect();
        Self {
            worker_id,
            worker_set,
            remote_versions,
            version: 0,
        }
    }

    /// Handle any data message reaching this operator
    fn handle_data<K, V, T>(
        &self,
        msg: DataMessage<K, V, T>,
        version: Option<Version>,
        partitioner: impl WorkerPartitioner<K>,
    ) -> OutgoingMessage<K, V, T> {
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

    fn handle_msg<K: Key, V, T>(
        mut self,
        msg: IncomingMessage<K, V, T>,
        partitioner: impl WorkerPartitioner<K>,
    ) -> (DistributorKind<K, V, T>, OutgoingMessage<K, V, T>) {
        match msg {
            IncomingMessage::Data(d, v) => {
                let result = self.handle_data(d, v, partitioner);
                (DistributorKind::<K, V, T>::Normal(self), result)
            }
            IncomingMessage::Rescale(trigger) => (
                DistributorKind::<K, V, T>::Interrogate(InterrogateDistributor::new(
                    self.worker_id,
                    self.worker_set,
                    self.version,
                    self.remote_versions,
                    trigger,
                )),
                OutgoingMessage::None,
            ),
            IncomingMessage::Done(d) => {
                self.remote_versions.insert(d.worker_id, d.version);
                (
                    DistributorKind::<K, V, T>::Normal(self),
                    OutgoingMessage::None,
                )
            }
        }
    }

    fn run<K, V, T>(
        self,
        _partitioner: impl WorkerPartitioner<K>,
    ) -> (DistributorKind<K, V, T>, OutgoingMessage<K, V, T>) {
        (
            DistributorKind::<K, V, T>::Normal(self),
            OutgoingMessage::None,
        )
    }
}

#[cfg(test)]
mod test {
    use crate::{
        keyed::distributed::messages::{
            DoneMessage, LocalOutgoingMessage, RemoteMessage, RescaleMessage, TargetedMessage,
        },
        snapshot::{Barrier, NoPersistence},
        test::CapturingPersistenceBackend,
        time::NoTime,
        DataMessage, NoData, NoKey,
    };

    use super::*;

    // a partitioner that just uses the key as a wrapping index
    fn partiton_index<'a>(i: &usize, s: &'a IndexSet<WorkerId>) -> &'a WorkerId {
        s.get_index(i % s.len()).unwrap()
    }

    #[test]
    /// Check data is forwared locally, if the dist_func says so
    fn forward_data_locally() {
        let dist = NormalDistributor::new(0, vec![]);

        let input: IncomingMessage<usize, i32, NoTime> = IncomingMessage::Data(
            crate::DataMessage {
                key: 0,
                value: 42,
                timestamp: NoTime,
            },
            None,
        );
        let input2: IncomingMessage<usize, i32, NoTime> = IncomingMessage::Data(
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

        let input: IncomingMessage<usize, i32, NoTime> = IncomingMessage::Data(
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

        let input: IncomingMessage<usize, i32, NoTime> = IncomingMessage::Data(
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

    #[test]
    /// Should update the corresponding worker version in its map
    fn handle_done_message() {
        let dist = NormalDistributor::new(0, vec![]);
        let input: IncomingMessage<usize, NoData, usize> =
            IncomingMessage::Done(DoneMessage::new(1, 42));
        // since there are no other
        let (dist, _) = dist.handle_msg(input, partiton_index);
        match dist {
            DistributorKind::Normal(d) => assert_eq!(d.remote_versions.get(&1).unwrap(), &42),
            _ => panic!("Wrong distributor returned"),
        }
    }

    #[test]
    fn handle_upscale() {
        let dist = NormalDistributor::new(0, vec![]);
        let input: IncomingMessage<usize, NoData, usize> =
            IncomingMessage::Rescale(RescaleMessage::ScaleAddWorker(IndexSet::from([1, 2, 3])));
        let output = dist.handle_msg(input, partiton_index);
        match output {
            (DistributorKind::Interrogate(_), OutgoingMessage::None) => (),
            _ => panic!("Wrong distributor returned"),
        }
    }

    #[test]
    fn handle_downscale() {
        let dist = NormalDistributor::new(0, vec![]);
        let input: IncomingMessage<usize, NoData, usize> =
            IncomingMessage::Rescale(RescaleMessage::ScaleRemoveWorker(IndexSet::from([1, 2, 3])));
        let output = dist.handle_msg(input, partiton_index);
        match output {
            (DistributorKind::Interrogate(_), OutgoingMessage::None) => (),
            _ => panic!("Wrong distributor returned"),
        }
    }

    #[test]
    fn handle_shutdown() {}
}
