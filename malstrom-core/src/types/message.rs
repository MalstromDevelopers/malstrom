//! Contains JetStream's message types.
//! JetStream communicates in between Operators exlusively via messages, which may contain
//! data or be control messages

use indexmap::IndexSet;
use serde::{Deserialize, Serialize};
use std::rc::Rc;

use crate::{
    keyed::distributed::{Acquire, Collect, Interrogate},
    snapshot::Barrier,
};

use super::{Timestamp, WorkerId};

/// A message which gets processed in a JetStream
/// Messages always include a timestamp and content.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DataMessage<K, V, T> {
    /// The key of the message. The message key controls how a message is distributed in a job
    /// with multiple workers. Also all state in Malstrom is keyed, so a message will (usually)
    /// only modify the state belonging to its key in stateful operators.
    pub key: K,
    /// Message value
    pub value: V,
    /// Message timestamp. Timestamps are logical and not necessarily related to real world time.
    /// Timestamps are useful to control ordering and out-of-orderness
    pub timestamp: T,
}
impl<K, V, T> DataMessage<K, V, T> {
    /// Create a new DataMessage from a key, value and timestamp
    pub fn new(key: K, value: V, timestamp: T) -> Self {
        Self {
            timestamp,
            key,
            value,
        }
    }
}
/// Content variants of a JetStream message.
/// Most messages will be of the data flavour, i.e. data to be processed,
/// however JetStream also uses its data channels to coordinate snapshoting
/// and rescaling
#[derive(Debug)]
pub enum Message<K, V, T> {
    /// A data record flowing through the data stream
    Data(DataMessage<K, V, T>),
    /// An epoch of the contained value. No messages with a timestamp less than or equal to the
    /// timestamp of this Epoch will follow
    Epoch(T),
    /// Barrier used for asynchronous snapshotting
    AbsBarrier(Barrier),
    /// Informational message that the job is currently rescaling
    Rescale(RescaleMessage),
    /// Information that this worker plans on shutting down (temporarily)
    /// See struct docstring for more information
    SuspendMarker(SuspendMarker),

    /// Rescaling state movement messages
    Interrogate(Interrogate<K>),
    /// Collect the current state for the key to be moved to another worker
    Collect(Collect<K>),
    /// Acquire the state for the key, i.e. add it to the state managed on this worker
    Acquire(Acquire<K>),
}
macro_rules! impl_from_variants {
    ($($variant:ident($variant_type:ty)),* $(,)?) => {
        $(
            impl<K, V, T> From<$variant_type> for Message<K, V, T> {
                fn from(value: $variant_type) -> Self {
                    Message::$variant(value)
                }
            }
        )*
    };
}
impl_from_variants!(
    Data(DataMessage<K, V, T>),
    AbsBarrier(Barrier),
    Rescale(RescaleMessage),
    SuspendMarker(SuspendMarker),
    Interrogate(Interrogate<K>),
    Collect(Collect<K>),
    Acquire(Acquire<K>),
);
impl<K, V, T> From<T> for Message<K, V, T>
where
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

impl<K, V, T> Clone for Message<K, V, T>
where
    K: Clone,
    V: Clone,
    T: Clone,
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
