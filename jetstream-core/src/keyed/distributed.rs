use std::{rc::Rc, sync::Mutex};

use indexmap::{IndexMap, IndexSet};

use crate::{
    channels::selective_broadcast::{Receiver, Sender},
    snapshot::{Barrier, Load, PersistenceBackend},
    stream::operator::OperatorContext,
    time::Timestamp,
    Data, DataMessage, Key, Message, OperatorId, WorkerId,
};

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use self::{
    collect::CollectDistributor, interrogate::InterrogateDistributor, normal::NormalDistributor,
};

use super::WorkerPartitioner;
mod collect;
mod interrogate;
mod normal;
// use super::normal_dist::NormalDistributor;

pub(super) type Version = usize;

/// Marker trait for distributable key
pub trait DistKey: Key + Serialize + DeserializeOwned + 'static {}
impl<T: Key + Serialize + DeserializeOwned + 'static> DistKey for T {}
/// Marker trait for distributable value
pub trait DistData: Data + Serialize + DeserializeOwned {}
impl<T: Data + Serialize + DeserializeOwned> DistData for T {}

pub trait DistTimestamp: Timestamp + Serialize + DeserializeOwned {}
impl<T: Timestamp + Serialize + DeserializeOwned> DistTimestamp for T {}

/// Enum which contains either data messages or instructions to change
/// the cluster scale
enum ScalableMessage<K, V, T> {
    Data(VersionedMessage<K, V, T>),
    /// Information that the worker of this ID will soon be removed
    /// from the computation. Triggers Rescaling procedure
    ScaleRemoveWorker(IndexSet<WorkerId>),
    /// Information that the worker of this ID will be added to the
    /// computation. Triggers Rescaling procedure
    ScaleAddWorker(IndexSet<WorkerId>),
    /// Sent by remote worker to indicate they are don with the current migration
    Done(WorkerId),
}

#[derive(Serialize, Deserialize)]
struct VersionedMessage<K, V, T> {
    pub version: Option<Version>,
    pub sender: WorkerId,
    pub message: DataMessage<K, V, T>,
}

#[derive(Serialize, Deserialize)]
enum NetworkMessage<K, V, T> {
    BarrierAlign(WorkerId),
    LoadAlign(WorkerId),
    Data(VersionedMessage<K, V, T>),
    Acquire(NetworkAcquire<K>),
    Done(WorkerId),
}

/// Enum of value distributors for different ICADD
/// phases
enum PhaseDistributor<K, V, T> {
    Normal(NormalDistributor),
    Interrogate(InterrogateDistributor<K, V, T>),
    Collect(CollectDistributor<K, V, T>),
    None,
}

pub(super) struct Distributor<K, V, T, P> {
    dist_func: Box<dyn WorkerPartitioner<K>>,
    worker_versions: IndexMap<WorkerId, Version>,
    inner: PhaseDistributor<K, V, T>,
    /// 0 elem is our locally received align, the set is for those received
    /// from remotes
    received_barriers: (Option<Barrier<P>>, IndexSet<WorkerId>),
    received_loads: (Option<Load<P>>, IndexSet<WorkerId>),
}

impl<K, V, T, P> Distributor<K, V, T, P>
where
    K: DistKey,
    V: DistData,
    T: DistTimestamp,
    P: PersistenceBackend,
{
    pub(super) fn new(dist_func: impl WorkerPartitioner<K>) -> Self {
        let inner = PhaseDistributor::Normal(NormalDistributor::default());
        Self {
            dist_func: Box::new(dist_func),
            worker_versions: IndexMap::new(),
            inner,
            received_barriers: (None, IndexSet::new()),
            received_loads: (None, IndexSet::new()),
        }
    }
    pub(super) fn run(
        &mut self,
        input: &mut Receiver<K, V, T, P>,
        output: &mut Sender<K, V, T, P>,
        ctx: &mut OperatorContext,
    ) {
        // TODO: check what if any state we need to persist
        let scalable_message = match input.recv() {
            Some(Message::AbsBarrier(b)) => {
                let _ = self.received_barriers.0.insert(b);
                ctx.communication
                    .broadcast(NetworkMessage::<K, V, T>::BarrierAlign(ctx.worker_id))
                    .expect("Communication error");
                None
            }
            Some(Message::Load(p)) => {
                let _ = self.received_loads.0.insert(p);
                ctx.communication
                    .broadcast(NetworkMessage::<K, V, T>::LoadAlign(ctx.worker_id))
                    .expect("Communication error");
                None
            }
            Some(Message::Data(d)) => Some(ScalableMessage::Data(VersionedMessage {
                sender: ctx.worker_id,
                version: None,
                message: d,
            })),
            Some(Message::ScaleAddWorker(x)) => Some(ScalableMessage::ScaleAddWorker(x)),
            Some(Message::ScaleRemoveWorker(x)) => Some(ScalableMessage::ScaleRemoveWorker(x)),
            Some(Message::ShutdownMarker(_x)) => {
                todo!()
            }
            // simply ignore all keying related messages, since they may not cross here
            _ => None,
        };
        self.call_distributor(scalable_message, output, ctx);

        for remote_message in ctx.communication.recv_all::<NetworkMessage<K, V, T>>() {
            match remote_message {
                NetworkMessage::BarrierAlign(x) => {
                    self.received_barriers.1.insert(x);
                }
                NetworkMessage::LoadAlign(x) => {
                    self.received_loads.1.insert(x);
                }
                NetworkMessage::Data(x) => {
                    self.call_distributor(Some(ScalableMessage::Data(x)), output, ctx);
                }
                NetworkMessage::Acquire(x) => output.send(Message::Acquire(x.into())),
                NetworkMessage::Done(x) => {
                    self.call_distributor(Some(ScalableMessage::Done(x)), output, ctx);
                }
            }
        }
        // synchronize barriers and loads
        if self.received_barriers.1.len() >= ctx.communication.get_peers().len() {
            if let Some(p) = self.received_barriers.0.take() {
                output.send(Message::AbsBarrier(p));
                self.received_barriers.1.drain(..);
            }
        }
        if self.received_loads.1.len() >= ctx.communication.get_peers().len() {
            if let Some(p) = self.received_loads.0.take() {
                output.send(Message::Load(p));
                self.received_loads.1.drain(..);
            }
        }
    }

    fn call_distributor(
        &mut self,
        msg: Option<ScalableMessage<K, V, T>>,
        output: &mut Sender<K, V, T, P>,
        ctx: &mut OperatorContext,
    ) {
        let inner = std::mem::replace(&mut self.inner, PhaseDistributor::None);
        self.inner = match inner {
            PhaseDistributor::Normal(x) => x.run(&self.dist_func, msg, output, ctx),
            PhaseDistributor::Interrogate(x) => x.run(&self.dist_func, msg, output, ctx),
            PhaseDistributor::Collect(x) => x.run(&self.dist_func, msg, output, ctx),
            PhaseDistributor::None => panic!("Invariant broken: Inner distributor can not be None"),
        }
    }
}

fn send_to_target<K: Clone, V: Clone, T: Clone, P>(
    msg: VersionedMessage<K, V, T>,
    target: &WorkerId,
    output: &mut Sender<K, V, T, P>,
    ctx: &OperatorContext,
) {
    if *target == ctx.worker_id {
        output.send(Message::Data(msg.message))
    }
}

#[derive(Debug, Clone)]
pub struct Interrogate<K> {
    shared: Rc<Mutex<IndexSet<K>>>,
}
impl<K> Interrogate<K>
where
    K: Key,
{
    pub fn add_keys(&mut self, keys: &[K]) {
        let mut guard = self.shared.lock().unwrap();
        for key in keys.iter().cloned() {
            guard.insert(key);
        }
    }

    pub(super) fn ref_count(&self) -> usize {
        Rc::strong_count(&self.shared)
    }
}

#[derive(Debug, Clone)]
pub struct Collect<K> {
    pub key: K,
    collection: Rc<Mutex<IndexMap<OperatorId, Vec<u8>>>>,
}
impl<K> Collect<K>
where
    K: Key,
{
    fn new(key: K) -> Self {
        Self {
            key,
            collection: Rc::new(Mutex::new(IndexMap::new())),
        }
    }
    pub fn add_state<S: Serialize>(&mut self, operator_id: OperatorId, state: &S) {
        let encoded = bincode::serde::encode_to_vec(state, bincode::config::standard())
            .expect("State serialization error");
        self.collection.lock().unwrap().insert(operator_id, encoded);
    }

    fn ref_count(&self) -> usize {
        Rc::strong_count(&self.collection)
    }
}

#[derive(Debug, Clone)]
pub struct Acquire<K> {
    key: K,
    collection: Rc<Mutex<IndexMap<OperatorId, Vec<u8>>>>,
}
impl<K> Acquire<K>
where
    K: Key,
{
    pub fn take_state<S: DeserializeOwned>(&self, operator_id: &OperatorId) -> Option<(K, S)> {
        self.collection
            .lock()
            .unwrap()
            .swap_remove(operator_id)
            .map(|x| {
                bincode::serde::decode_from_slice(&x, bincode::config::standard())
                    .expect("State deserialization error")
                    .0
            })
            .map(|s| (self.key.clone(), s))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct NetworkAcquire<K> {
    key: K,
    collection: IndexMap<OperatorId, Vec<u8>>,
}

// impl<K> From<Acquire<K>> for NetworkAcquire<K> {
//     fn from(value: Acquire<K>) -> Self {
//         Self {
//             key: value.key,
//             collection: value.collection.into_inner().unwrap(),
//         }
//     }
// }
impl<K> From<NetworkAcquire<K>> for Acquire<K> {
    fn from(value: NetworkAcquire<K>) -> Self {
        Self {
            key: value.key,
            collection: Rc::new(Mutex::new(value.collection)),
        }
    }
}

#[derive(Serialize, Deserialize)]
struct DoneMessage {
    worker_id: WorkerId,
    version: Version,
}
