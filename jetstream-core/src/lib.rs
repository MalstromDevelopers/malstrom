use indexmap::IndexSet;
use keyed::distributed::{Acquire, Collect, Interrogate};
use serde_derive::{Deserialize, Serialize};
use snapshot::Barrier;

use std::{hash::Hash, rc::Rc};
// use time::Epoch;
mod util;

pub mod channels;
pub mod config;
pub mod keyed;
pub mod operators;
pub mod sinks;
pub mod snapshot;
pub mod sources;
pub mod stream;
pub mod test;
pub mod time;
pub mod worker;

type OperatorId = usize;
type WorkerId = usize;
type Scale = usize;

/// Marker trait for stream keys
pub trait Key: Hash + Eq + PartialEq + Clone + 'static {}
impl<T: Hash + Eq + PartialEq + Clone + 'static> Key for T {}

/// Marker trait to denote streams that may or may not be keyed
pub trait MaybeKey: Clone + 'static {}
impl<T: Clone + 'static> MaybeKey for T {}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NoKey;

/// Data which may move through a stream
pub trait Data: Clone + 'static {}
impl<T: Clone + 'static> Data for T {}

/// Marker trait to denote streams that may or may not have data
pub trait MaybeData: Clone + 'static {}
impl<T: Clone + 'static> MaybeData for T {}

/// Zero sized indicator for a stream with no data
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct NoData;

/// Marker trait for functions which determine inter-operator routing
pub trait OperatorPartitioner<K, V, T>:
    Fn(&DataMessage<K, V, T>, Scale) -> Vec<OperatorId> + 'static
{
}
impl<K, V, T, U: Fn(&DataMessage<K, V, T>, Scale) -> Vec<OperatorId> + 'static>
    OperatorPartitioner<K, V, T> for U
{
}

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
