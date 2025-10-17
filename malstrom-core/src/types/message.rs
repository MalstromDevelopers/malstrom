//! Contains JetStream's message types.
//! JetStream communicates in between Operators exlusively via messages, which may contain
//! data or be control messages

use indexmap::IndexSet;
use serde::{Deserialize, Serialize, ser::SerializeStruct};
use std::{fmt::Debug, rc::Rc};

use crate::{
    keyed::distributed::{Acquire, Collect, Interrogate},
    snapshot::Barrier,
    types::{MaybeData, MaybeKey, MaybeTime, NoData, NoKey, NoTime},
};

use super::{Timestamp, WorkerId};

/// A helper trait which saves us from specifying the key, value and timestamp generics
/// everywhere
pub trait Kvt: Clone + 'static {
    type Key: MaybeKey;
    type Value: MaybeData;
    type Timestamp: MaybeTime;
}

impl<K, V, T> Kvt for (K, V, T)
where
    K: MaybeKey,
    V: MaybeData,
    T: MaybeTime,
{
    type Key = K;
    type Value = V;
    type Timestamp = T;
}

impl Kvt for () {
    type Key = NoKey;
    type Value = NoData;
    type Timestamp = NoTime;
}

#[macro_export]
macro_rules! msg {
    ($kvt:ty) => {
        (
            <$kvt as Kvt>::Key,
            <$kvt as Kvt>::Value,
            <$kvt as Kvt>::Timestamp,
        )
    };
}

/// A message which gets processed in a JetStream
/// Messages always include a timestamp and content.
#[derive(Clone, Serialize, Deserialize)]
pub struct DataMessage<M: Kvt> {
    /// The key of the message. The message key controls how a message is distributed in a job
    /// with multiple workers. Also all state in Malstrom is keyed, so a message will (usually)
    /// only modify the state belonging to its key in stateful operators.
    #[serde(bound(
        serialize = "<M as Kvt>::Key: Serialize",
        deserialize = "<M as Kvt>::Key: Deserialize<'de>"
    ))]
    pub key: <M as Kvt>::Key,
    /// Message value
    #[serde(bound(
        serialize = "<M as Kvt>::Value: Serialize",
        deserialize = "<M as Kvt>::Value: Deserialize<'de>"
    ))]
    pub value: <M as Kvt>::Value,
    /// Message timestamp. Timestamps are logical and not necessarily related to real world time.
    /// Timestamps are useful to control ordering and out-of-orderness
    #[serde(bound(
        serialize = "<M as Kvt>::Timestamp: Serialize",
        deserialize = "<M as Kvt>::Timestamp: Deserialize<'de>"
    ))]
    pub timestamp: <M as Kvt>::Timestamp,
}
impl<M: Kvt> DataMessage<M> {
    /// Create a new DataMessage from a key, value and timestamp
    pub fn new(
        key: <M as Kvt>::Key,
        value: <M as Kvt>::Value,
        timestamp: <M as Kvt>::Timestamp,
    ) -> Self {
        Self {
            timestamp,
            key,
            value,
        }
    }
}

impl<M> Debug for DataMessage<M>
where
    M: Kvt,
    M::Key: Debug,
    M::Value: Debug,
    M::Timestamp: Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataMessage")
            .field("key", &self.key)
            .field("value", &self.value)
            .field("timestamp", &self.timestamp)
            .finish()
    }
}
impl<M> PartialEq for DataMessage<M>
where
    M: Kvt,
    M::Key: PartialEq,
    M::Value: PartialEq,
    M::Timestamp: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.value == other.value && self.timestamp == other.timestamp
    }
}

/// Content variants of a JetStream message.
/// Most messages will be of the data flavour, i.e. data to be processed,
/// however JetStream also uses its data channels to coordinate snapshoting
/// and rescaling
pub enum Message<M: Kvt> {
    /// A data record flowing through the data stream
    Data(DataMessage<M>),
    /// An epoch of the contained value. No messages with a timestamp less than or equal to the
    /// timestamp of this Epoch will follow
    Epoch(<M as Kvt>::Timestamp),
    /// Barrier used for asynchronous snapshotting
    AbsBarrier(Barrier),
    /// Informational message that the job is currently rescaling
    Rescale(RescaleMessage),
    /// Information that this worker plans on shutting down (temporarily)
    /// See struct docstring for more information
    SuspendMarker(SuspendMarker),

    /// Rescaling state movement messages
    Interrogate(Interrogate<<M as Kvt>::Key>),
    /// Collect the current state for the key to be moved to another worker
    Collect(Collect<<M as Kvt>::Key>),
    /// Acquire the state for the key, i.e. add it to the state managed on this worker
    Acquire(Acquire<<M as Kvt>::Key>),
}

impl<M> Debug for Message<M>
where
    M: Kvt,
    M::Key: Debug,
    M::Value: Debug,
    M::Timestamp: Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Data(arg0) => f.debug_tuple("Data").field(arg0).finish(),
            Self::Epoch(arg0) => f.debug_tuple("Epoch").field(arg0).finish(),
            Self::AbsBarrier(arg0) => f.debug_tuple("AbsBarrier").field(arg0).finish(),
            Self::Rescale(arg0) => f.debug_tuple("Rescale").field(arg0).finish(),
            Self::SuspendMarker(arg0) => f.debug_tuple("SuspendMarker").field(arg0).finish(),
            Self::Interrogate(arg0) => f.debug_tuple("Interrogate").field(arg0).finish(),
            Self::Collect(arg0) => f.debug_tuple("Collect").field(arg0).finish(),
            Self::Acquire(arg0) => f.debug_tuple("Acquire").field(arg0).finish(),
        }
    }
}

macro_rules! impl_from_variants {
    ($($variant:ident($variant_type:ty)),* $(,)?) => {
        $(
            impl<M, K, V, T> From<$variant_type> for Message<M>
            where
                M: Kvt<Key = K, Value = V, Timestamp = T>,
                K: MaybeKey,
                V: MaybeData,
                T: MaybeTime,
            {
                fn from(value: $variant_type) -> Self {
                    Message::$variant(value)
                }
            }
        )*
    };
}
impl_from_variants!(
    Data(DataMessage<M>),
    AbsBarrier(Barrier),
    Rescale(RescaleMessage),
    SuspendMarker(SuspendMarker),
    Interrogate(Interrogate<K>),
    Collect(Collect<K>),
    Acquire(Acquire<K>),
);
impl<M, T> From<T> for Message<M>
where
    M: Kvt<Timestamp = T>,
    T: Timestamp,
{
    fn from(value: T) -> Self {
        Message::Epoch(value)
    }
}

/// Indicates a reconfiguration in the amount of workers
/// participating in the computation
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct RescaleMessage {
    /// Set of workers in the computation AFTER the rescale
    /// will have concluded
    workers: IndexSet<WorkerId>,
    version: u64,
    rc: Rc<()>,
}

impl RescaleMessage {
    pub(crate) fn new(workers: IndexSet<WorkerId>, version: u64) -> Self {
        Self {
            workers,
            version,
            rc: Rc::new(()),
        }
    }

    /// Get the set of workers which will be active after the rescale
    /// has concluded
    pub fn get_new_workers(&self) -> &IndexSet<WorkerId> {
        &self.workers
    }

    /// Get the version of this rescaling
    pub fn get_version(&self) -> u64 {
        self.version
    }

    /// Get the count of strong reference to the inner Rc
    /// Note that this includes the instance you are calling
    /// this method on.
    pub(crate) fn strong_count(&self) -> usize {
        Rc::strong_count(&self.rc)
    }
}

impl<M> Clone for Message<M>
where
    M: Kvt + Clone,
{
    fn clone(&self) -> Self {
        // for some reason this could not be derived
        match self {
            Self::Data(x) => Self::Data(x.clone()),
            Self::Epoch(x) => Self::Epoch(x.clone()),
            Self::AbsBarrier(x) => Self::AbsBarrier(x.clone()),
            Self::Rescale(x) => Self::Rescale(x.clone()),
            Self::SuspendMarker(x) => Self::SuspendMarker(x.clone()),
            Self::Interrogate(x) => Self::Interrogate(x.clone()),
            Self::Collect(x) => Self::Collect(x.clone()),
            Self::Acquire(x) => Self::Acquire(x.clone()),
        }
    }
}

/// This marker will be sent by the cluster lifecycle controller
/// when the worker is planning to shut down.
/// Operators wishing to delay shut down, must hold onto this marker as long
/// as necessary
#[derive(Debug, Clone, Default)]
pub struct SuspendMarker {
    rc: Rc<()>,
}
impl SuspendMarker {
    /// Get the count of strong reference to the inner Rc
    /// Note that this includes the instance you are calling
    /// this method on.
    pub(crate) fn strong_count(&self) -> usize {
        Rc::strong_count(&self.rc)
    }
}
