use std::iter;

use indexmap::{IndexMap, IndexSet};
use serde::{Deserialize, Serialize};

use crate::{
    keyed::WorkerPartitioner, snapshot::PersistenceBackend, stream::operator::BuildContext,
    DataMessage, Key, RescaleMessage, WorkerId,
};

use super::{
    dist_trait::DistributorKind,
    interrogate::InterrogateDistributor,
    messages::{LocalOutgoingMessage, OutgoingMessage, RemoteMessage, VersionedMessage},
    Version,
};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub(super) struct NormalDistributor {
    pub(super) worker_id: WorkerId,
    pub(super) worker_set: IndexSet<WorkerId>,
    pub(super) version: Version,
}
impl NormalDistributor {
    pub(super) fn new(worker_id: WorkerId, worker_set: IndexSet<WorkerId>) -> Self {
        Self {
            worker_id,
            worker_set,
            version: 0,
        }
    }

    pub(super) fn into_interrogate<K: Key>(
        self,
        trigger: RescaleMessage,
    ) -> InterrogateDistributor<K> {
        InterrogateDistributor::new(self.worker_id, self.worker_set, self.version, trigger)
    }

    pub(super) fn handle_msg<K: Key, V, T>(
        &self,
        msg: DataMessage<K, V, T>,
        msg_version: Option<Version>,
        partitioner: &dyn WorkerPartitioner<K>,
    ) -> OutgoingMessage<K, V, T> {
        if msg_version.map(|v| v > self.version).unwrap_or(false) {
            return OutgoingMessage::Local(LocalOutgoingMessage::Data(msg));
        }
        let target = partitioner(&msg.key, &self.worker_set);
        if *target == self.worker_id {
            OutgoingMessage::Local(LocalOutgoingMessage::Data(msg))
        } else {
            let tm = VersionedMessage::new(self.version, msg);
            OutgoingMessage::Remote(*target, tm)
        }
    }
}

#[cfg(test)]
mod test {
    use anyhow::Result;

    use crate::{
        keyed::distributed::messages::{
            DoneMessage, LocalOutgoingMessage, RemoteMessage, VersionedMessage,
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
        let mut dist = NormalDistributor::new(0, IndexSet::from([0]));

        let input = DataMessage {
            key: 0,
            value: 42,
            timestamp: NoTime,
        };
        let input2 = DataMessage {
            key: 0,
            value: 42,
            timestamp: NoTime,
        };

        let result = dist.handle_msg(input, None, &partiton_index);
        assert!(matches!(
            result,
            OutgoingMessage::Local(LocalOutgoingMessage::Data(DataMessage {
                key: 0,
                value: 42,
                timestamp: NoTime
            }))
        ));

        let result = dist.handle_msg(input2, None, &partiton_index);
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
        let mut dist = NormalDistributor::new(0, IndexSet::from([0, 1]));

        let input = DataMessage {
            key: 1,
            value: 42,
            timestamp: NoTime,
        };
        let result = dist.handle_msg(input, None, &partiton_index);

        assert!(matches!(
            result,
            OutgoingMessage::Remote(1, VersionedMessage {
                version: 0,
                message: DataMessage {
                    key: 1,
                    value: 42,
                    timestamp: NoTime
                }
            })
        ));
    }

    #[test]
    /// Check data is ALWAYS forwarded locally, if the version is higher
    fn forward_local_higher_version() {
        let dist = NormalDistributor::new(0, IndexSet::from([0, 1]));

        let input = DataMessage {
            key: 1,
            value: 42,
            timestamp: NoTime,
        };
        let result = dist.handle_msg(input, Some(1), &partiton_index);
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
