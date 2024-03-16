use indexmap::IndexSet;
use keyed::distributed::{Acquire, Collect, Interrogate};
use serde_derive::{Deserialize, Serialize};
use snapshot::{Barrier, Load};

use std::{hash::Hash, rc::Rc};
use time::Epoch;
mod util;

pub mod channels;
pub mod config;
pub mod keyed;
pub mod operators;
pub mod snapshot;
pub mod sources;
pub mod stream;
pub mod time;
pub mod worker;
pub mod sinks;

pub use worker::Worker;

type OperatorId = usize;
type WorkerId = usize;
type Scale = usize;

/// Marker trait for stream keys
pub trait Key: Hash + Eq + PartialEq + Clone + 'static {}
impl<T: Hash + Eq + PartialEq + Clone + 'static> Key for T {}

/// Marker trait to denote streams that may or may not be keyed
pub trait MaybeKey: Clone + 'static {}
impl<T: Clone + 'static> MaybeKey for T {}

#[derive(Clone)]
pub struct NoKey;

/// Data which may move through a stream
pub trait Data: Clone + 'static {}
impl<T: Clone + 'static> Data for T {}

// /// Zero sized indicator for an unkeyed stream
// #[derive(Clone)]
// pub struct NoKey;
// impl Key for NoKey {
//     type KeyType = ()
// }

/// Zero sized indicator for a stream with no data
#[derive(Clone)]
pub struct NoData;

/// Marker trait for functions which determine inter-operator routing
pub trait OperatorPartitioner<K, V, T>:
    Fn(&DataMessage<K, V, T>, Scale) -> IndexSet<OperatorId> + 'static
{
}
impl<K, V, T, U: Fn(&DataMessage<K, V, T>, Scale) -> IndexSet<OperatorId> + 'static>
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
        Self { timestamp, key, value }
    }
}
/// Content variants of a JetStream message.
/// Most messages will be of the data flavour, i.e. data to be processed,
/// however JetStream also uses its data channels to coordinate snapshoting
/// and rescaling
#[derive(Debug)]
pub enum Message<K, V, T, P> {
    Data(DataMessage<K, V, T>),
    Epoch(Epoch<T>),
    /// Barrier used for asynchronous snapshotting
    AbsBarrier(Barrier<P>),
    /// Instruction to load state
    Load(Load<P>),
    /// Information that the worker of this ID will soon be removed
    /// from the computation. Triggers Rescaling procedure
    ScaleRemoveWorker(IndexSet<WorkerId>),
    /// Information that the worker of this ID will be added to the
    /// computation. Triggers Rescaling procedure
    ScaleAddWorker(IndexSet<WorkerId>),
    /// Information that this worker plans on shutting down
    /// See struct docstring for more information
    ShutdownMarker(ShutdownMarker),

    /// Rescaling state movement messages
    Interrogate(Interrogate<K>),
    Collect(Collect<K>),
    Acquire(Acquire<K>),
    DropKey(K),
}

impl<K, V, T, P> Clone for Message<K, V, T, P>
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
            Self::Load(x) => Self::Load(x.clone()),
            Self::ScaleRemoveWorker(x) => Self::ScaleRemoveWorker(x.clone()),
            Self::ScaleAddWorker(x) => Self::ScaleAddWorker(x.clone()),
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
#[derive(Debug, Clone)]
pub struct ShutdownMarker {
    rc: Rc<()>,
}
impl ShutdownMarker {
    /// Get the count of strong reference to the inner Rc
    /// Note that this includes the instance you are calling
    /// this method on.
    pub(crate) fn get_count(&self) -> usize {
        Rc::strong_count(&self.rc)
    }
}
