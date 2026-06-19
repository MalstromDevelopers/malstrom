//! Contains JetStream's message types.
//! JetStream communicates in between Operators exlusively via messages, which may contain
//! data or be control messages

use futures::FutureExt;
use indexmap::IndexSet;
use serde::{Deserialize, Serialize, de::DeserializeOwned, ser::SerializeStruct};
use std::{cell::RefCell, fmt::Debug, rc::Rc};
use tokio::sync::mpsc;

use crate::{
    keyed::distributed::{Acquire, Collect, Interrogate},
    snapshot::SnapshotBarrier,
    types::{MaybeData, MaybeKey, MaybeTime, NoData, NoKey, NoTime, OperatorId},
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

/// A message which gets processed in Malstrom
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
#[derive(Clone)]
pub enum Message<M: Kvt> {
    /// A data record flowing through the data stream
    Data(DataMessage<M>),
    /// An epoch of the contained value. No messages with a timestamp less than or equal to the
    /// timestamp of this Epoch will follow
    Epoch(<M as Kvt>::Timestamp),
    /// Barrier used for asynchronous snapshotting
    AbsBarrier(Barrier),
    /// Informational message that the job is currently rescaling
    /// TODO: Rename Reconfig
    Rescale(RescaleMessage),
    /// Information that this worker plans on shutting down (temporarily)
    /// See struct docstring for more information
    // SuspendMarker(SuspendMarker),

    /// Information that job reconfiguration has completed with new ConfigVersion
    ReconfigComplete(ReconfigComplete),

    /// Rescaling state movement messages
    Interrogate(Interrogate<<M as Kvt>::Key>),
    /// Collect the current state for the key to be moved to another worker
    Collect(Collect<<M as Kvt>::Key>),
    /// Acquire the state for the key, i.e. add it to the state managed on this worker
    Acquire(Acquire<<M as Kvt>::Key>),
}

#[derive(Debug, Clone)]
pub enum Barrier {
    // Take a snapshot, then suspend
    Suspend(SnapshotBarrier),
    /// Take a snapshot, then resume
    Snapshot(SnapshotBarrier),
}

impl Barrier {
    /// Persist the given state for the given operator.
    pub fn persist<S: Serialize + DeserializeOwned>(
        &mut self,
        state: &S,
        operator_id: &OperatorId,
    ) {
        match self {
            Barrier::Suspend(snapshot_barrier) => snapshot_barrier.persist(state, operator_id),
            Barrier::Snapshot(snapshot_barrier) => snapshot_barrier.persist(state, operator_id),
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

/// Indicates a reconfiguration in the amount of workers
/// participating in the computation
#[derive(Debug, Clone)]
pub struct RescaleMessage {
    /// Set of workers in the computation AFTER the rescale
    /// will have concluded
    workers: IndexSet<WorkerId>,
    version: u64,
    callback: Rc<RefCell<mpsc::Sender<()>>>,
}

impl RescaleMessage {
    pub(crate) fn new(
        workers: IndexSet<WorkerId>,
        version: u64,
        callback: mpsc::Sender<()>,
    ) -> Self {
        Self {
            workers,
            version,
            callback: Rc::new(RefCell::new(callback)),
        }
    }

    /// Get the set of workers which will be active after the rescale
    /// has concluded
    pub fn get_all_workers(&self) -> &IndexSet<WorkerId> {
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
        Rc::strong_count(&self.callback)
    }
}

#[derive(Clone)]
pub struct ReconfigComplete {
    /// Configuration version we have advanced to
    version: u64,
    /// Set of workerIds in the new configuration
    workers: IndexSet<WorkerId>,
}

impl ReconfigComplete {
    pub fn get_new_worker_set(&self) -> &IndexSet<WorkerId> {
        &self.workers
    }
    pub fn get_new_version(&self) -> u64 {
        self.version
    }
}

/// This marker will be sent by the cluster lifecycle controller
/// when the worker is planning to shut down.
/// Operators wishing to delay shut down, must hold onto this marker as long
/// as necessary
#[derive(Debug, Clone)]
pub struct SuspendMarker {
    callback: Rc<RefCell<mpsc::Sender<()>>>,
}
impl SuspendMarker {
    pub(crate) fn new(callback: mpsc::Sender<()>) -> Self {
        SuspendMarker {
            callback: Rc::new(RefCell::new(callback)),
        }
    }
}

impl Drop for SuspendMarker {
    fn drop(&mut self) {
        if Rc::strong_count(&self.callback) == 1 {
            self.callback.borrow_mut().send(()).now_or_never().unwrap();
        }
    }
}
