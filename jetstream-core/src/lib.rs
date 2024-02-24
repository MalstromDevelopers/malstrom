
use frontier::Timestamp;
use indexmap::IndexSet;
use keyed::distributed::{Acquire, Collect, Interrogate};
use serde_derive::{Deserialize, Serialize};
use std::{hash::Hash, rc::Rc};

mod channels;
// pub mod filter;
pub mod frontier;
// pub mod inspect;
// pub mod kafka;
// pub mod map;
pub mod config;
// pub mod network_exchange;
pub mod snapshot;
// pub mod source;
// pub mod stateful_map;
pub mod keyed;
pub mod stream;
pub mod worker;

type OperatorId = usize;
type WorkerId = usize;
type Scale = usize;

/// Marker trait for stream keys
trait Key: Hash + Eq + PartialEq + Clone + 'static {}
impl<T: Hash + Eq + PartialEq + Clone + 'static> Key for T {}

/// Data which may move through a stream
pub trait Data: Clone + 'static {}
impl<T: Clone + 'static> Data for T {}

/// Zero sized indicator for an unkeyed stream
#[derive(Clone, Hash, PartialEq, Eq)]
struct NoKey;
/// Zero sized indicator for a stream with no data
#[derive(Clone)]
struct NoData;

/// Marker trait for functions which determine inter-operator routing
trait OperatorPartitioner<K, T>: Fn(&DataMessage<K, T>, Scale) -> IndexSet<OperatorId> + 'static {}
impl<K, T, U: Fn(&DataMessage<K, T>, Scale) ->  IndexSet<OperatorId> + 'static> OperatorPartitioner<K, T>
    for U
{
}

/// A message which gets processed in a JetStream
/// Messages always include a timestamp and content.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataMessage<K, T> {
    pub time: Timestamp,
    pub key: K,
    pub value: T,
}
/// Content variants of a JetStream message.
/// Most messages will be of the data flavour, i.e. data to be processed,
/// however JetStream also uses its data channels to coordinate snapshoting
/// and rescaling
#[derive(Debug, Clone)]
pub enum Message<K, T, P> {
    Data(DataMessage<K, T>),
    /// Barrier used for asynchronous snapshotting
    AbsBarrier(P),
    /// Instruction to load state
    Load(P),
    /// Information that the worker of this ID will soon be removed
    /// from the computation. Triggers Rescaling procedure
    ScaleRemoveWorker(IndexSet<WorkerId>),
    /// Information that the worker of this ID will be added to the
    /// computation. Triggers Rescaling procedure
    ScaleAddWorker(IndexSet<WorkerId>),
    /// Information that this worker plans on shutting down
    /// See struct docstring for more information
    ShutdownMarker(ShutdownMarker),

    Interrogate(Interrogate<K>),
    Collect(Collect<K>),
    Acquire(Acquire<K>),
    DropKey(K),
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
    fn get_count(&self) -> usize {
        Rc::strong_count(&self.rc)
    }
}
