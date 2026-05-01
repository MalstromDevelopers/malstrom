use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

use crate::{keyed::distributed::{Acquire, versioned_message::{VersionedData, VersionedMessage}}, types::{DataMessage, Kvt, Message, OperatorId}};
use crate::types::distributable::Distributable;

/// The message sent acroos Worker boundaries to communicate between workers
#[derive(Serialize, Deserialize, Clone)]
#[serde(bound = "M::Key: Distributable, M::Value: Distributable, M::Timestamp: Distributable")]
pub(super) enum WireMessage<M: Kvt +> {
    Data(VersionedData<M>),
    Epoch(<M as Kvt>::Timestamp),
    SnapshotBarrier,
    Acquire(WireAcquire<<M as Kvt>::Key>),
}

/// Serializable packaged version of Acquire, contains all collected state for a key
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct WireAcquire<K> {
    pub(super) key: K,
    pub(super) collection: IndexMap<OperatorId, Vec<u8>>,
}

impl<K> WireAcquire<K> {
    pub(super) fn new(key: K, collection: IndexMap<OperatorId, Vec<u8>>) -> Self {
        Self { key, collection }
    }
}
