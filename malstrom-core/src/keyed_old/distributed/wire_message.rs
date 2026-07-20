use std::{rc::Rc, sync::Mutex};

use indexmap::{IndexMap, IndexSet};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::{
    keyed::distributed::{Acquire, Collect},
    runtime::communication::Distributable,
    snapshot::SnapshotVersion,
    types::{DataMessage, Key, Kvt, MaybeData, MaybeTime, Message, OperatorId, WorkerId},
};

/// Marker trait for distributable key
pub trait DistKey: Key + Distributable {}
impl<T: Key + Distributable> DistKey for T {}
/// Marker trait for distributable value
pub trait DistData: MaybeData + Distributable {}
impl<T: MaybeData + Distributable> DistData for T {}
/// A timestamp which can be sent to other workers
pub trait DistTimestamp: MaybeTime + Distributable {}
impl<T: MaybeTime + Distributable> DistTimestamp for T {}

pub(super) type Version = u64;

pub(super) type VersionedMessage<M: Kvt> =
    Message<(M::Key, (M::Value, Version, WorkerId), M::Timestamp)>;
    
pub(super) type VersionedDataMessage<M: Kvt> = DataMessage<(M::Key, (M::Value, Version, WorkerId), M::Timestamp)>;

#[derive(Serialize, Deserialize, Clone)]
pub(super) enum WireMessage<M: Kvt> {
    #[serde(bound(
        serialize = "M::Key: Serialize, M::Value: Serialize, M::Timestamp: Serialize",
        deserialize = "M::Key: Deserialize<'de>, M::Value: Deserialize<'de>, M::Timestamp: Deserialize<'de>"
    ))]
    Data(VersionedDataMessage<M>),
    Epoch(<M as Kvt>::Timestamp),
    SnapshotBarrier,
    Acquire(WireAcquire<<M as Kvt>::Key>),
}

impl<M> WireMessage<M>
where
    M: Kvt,
    M::Key: Serialize + DeserializeOwned,
    M::Value: Serialize + DeserializeOwned,
    M::Timestamp: Serialize + DeserializeOwned,
{
    pub(super) fn is_barrier(&self) -> bool {
        matches!(self, WireMessage::SnapshotBarrier)
    }
}

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
