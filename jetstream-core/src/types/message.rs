//! Contains JetStream's message types.
//! JetStream communicates in between Operators exlusively via messages, which may contain
//! data or be control messages

use indexmap::IndexSet;
use serde::{Deserialize, Serialize};
use std::rc::Rc;

use crate::{keyed::distributed::{Acquire, Collect, Interrogate}, snapshot::Barrier};

use super::WorkerId;


/// A message which gets processed in a JetStream
/// Messages always include a timestamp and content.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataMessage<K, V, T> {
    pub key: K,
    pub value: V,
    pub timestamp: T,
}
impl<K, V, T> DataMessage<K, V, T> {
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
    Data(DataMessage<K, V, T>),
    Epoch(T),
    /// Barrier used for asynchronous snapshotting
    AbsBarrier(Barrier),
    Rescale(RescaleMessage),
    /// Information that this worker plans on shutting down
    /// See struct docstring for more information
    ShutdownMarker(ShutdownMarker),

    /// Rescaling state movement messages
    Interrogate(Interrogate<K>),
    Collect(Collect<K>),
    Acquire(Acquire<K>),
    DropKey(K),
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub enum RescaleMessage {
    ScaleRemoveWorker(IndexSet<WorkerId>),
    ScaleAddWorker(IndexSet<WorkerId>),
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
            // Self::Load(x) => Self::Load(x.clone()),
            Self::Rescale(x) => Self::Rescale(x.clone()),
            Self::ShutdownMarker(x) => Self::ShutdownMarker(x.clone()),
            Self::Interrogate(x) => Self::Interrogate(x.clone()),
            Self::Collect(x) => Self::Collect(x.clone()),
            Self::Acquire(x) => Self::Acquire(x.clone()),
            Self::DropKey(x) => Self::DropKey(x.clone()),
        }
    }
}

/// This marker will be sent by the cluster lifecycle controller
/// when the worker is planning to shut down.
/// The Marker is internally reference counted. When the count of strong references
/// to this marker goes to 0, the worker will shut down. Operators wishing
/// to delay shut down, must therefore hold onto a clone of this marker as long
/// as necessary
#[derive(Debug, Clone, Default)]
pub struct ShutdownMarker {
    rc: Rc<()>,
}
impl ShutdownMarker {
    /// Get the count of strong reference to the inner Rc
    /// Note that this includes the instance you are calling
    /// this method on.
    pub(crate) fn strong_count(&self) -> usize {
        Rc::strong_count(&self.rc)
    }
}
