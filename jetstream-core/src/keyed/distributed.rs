use std::{marker::PhantomData, rc::Rc, sync::Mutex};

use bincode::config::Configuration;
use indexmap::{IndexMap, IndexSet};

use crate::{
    channels::selective_broadcast::{Receiver, Sender},
    snapshot::PersistenceBackend,
    stream::operator::OperatorContext,
    Data, DataMessage, Key, Message, OperatorId, WorkerId,
};

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

// use super::normal_dist::NormalDistributor;

pub(super) type Version = usize;

/// Marker trait for distributable key
pub trait DistKey: Key + Serialize + DeserializeOwned {}
impl<T: Key + Serialize + DeserializeOwned> DistKey for T {}
/// Marker trait for distributable value
pub trait DistData: Data + Serialize + DeserializeOwned {}
impl<T: Data + Serialize + DeserializeOwned> DistData for T {}

/// Enum which contains either data messages or instructions to change
/// the cluster scale
pub(super) enum ScalableMessage<K, T> {
    Data(VersionedMessage<K, T>),
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
pub(super) struct VersionedMessage<K, T> {
    pub version: Option<Version>,
    pub message: DataMessage<K, T>,
}

#[derive(Serialize, Deserialize)]
pub(super) enum NetworkMessage<K, T> {
    BarrierAlign(WorkerId),
    LoadAlign(WorkerId),
    Data(VersionedMessage<K, T>),
    Acquire(NetworkAcquire<K>),
    Done(WorkerId),
}

/// Enum of value distributors for different ICADD
/// phases
pub(super) enum PhaseDistributor<K, T> {
    Normal(NormalDistributor),
    Interrogate(InterrogateDistributor<K, T>),
    Collect(CollectDistributor<K, T>),
}

struct Distributor<K, T, P> {
    dist_func: Box<dyn Fn(&K, &IndexSet<WorkerId>) -> WorkerId>,
    version: Version,
    worker_versions: IndexMap<WorkerId, Version>,
    inner: PhaseDistributor<K, T>,
    /// 0 elem is our locally received align, the set is for those received
    /// from remotes
    received_barriers: (Option<P>, IndexSet<WorkerId>),
    received_loads: (Option<P>, IndexSet<WorkerId>),
}

impl<K, T, P> Distributor<K, T, P>
where
K: DistKey,
T: DistData,
    P: PersistenceBackend,
{
    fn run(
        &mut self,
        input: &mut Receiver<K, T, P>,
        output: &mut Sender<K, T, P>,
        mut ctx: OperatorContext,
    ) {
        // TODO: check what if any state we need to persist
        let scalable_message = match input.recv() {
            Some(Message::AbsBarrier(mut b)) => {
                // persist frontier
                b.persist(ctx.frontier.get_actual(), &(), ctx.operator_id);
                let _ = self.received_barriers.0.insert(b);
                ctx.communication
                    .broadcast(postbox::Message::new(
                        ctx.operator_id,
                        NetworkMessage::<K, T>::BarrierAlign(ctx.worker_id),
                    ))
                    .expect("Communication error");
                None
            }
            Some(Message::Load(p)) => {
                // load frontier
                ctx.frontier
                    .advance_to(p.load::<()>(ctx.operator_id).unwrap_or_default().0);
                let _ = self.received_loads.0.insert(p);
                ctx.communication
                    .broadcast(postbox::Message::new(
                        ctx.operator_id,
                        NetworkMessage::<K, T>::LoadAlign(ctx.worker_id),
                    ))
                    .expect("Communication error");
                None
            }
            Some(Message::Data(d)) => Some(ScalableMessage::Data(VersionedMessage {
                version: None,
                message: d,
            })),
            Some(Message::ScaleAddWorker(x)) => Some(ScalableMessage::ScaleAddWorker(x)),
            Some(Message::ScaleRemoveWorker(x)) => Some(ScalableMessage::ScaleRemoveWorker(x)),
            Some(Message::ShutdownMarker(x)) => {
                todo!()
            }
            // simply ignore all keying related messages, since they may not cross here
            _ => None,
        };
        if let Some(msg) = scalable_message {
            self.inner = match self.inner {
                PhaseDistributor::Normal(x) => x.handle_msg(self.dist_func, msg, output, &mut ctx),
                PhaseDistributor::Interrogate(x) => x.handle_msg(self.dist_func, msg, output, &mut ctx),
                PhaseDistributor::Collect(_) => todo!(),
            };
        }
    }
}

#[derive(Serialize, Deserialize)]
pub(super) struct NormalDistributor {
    // includes local worker
    pub worker_set: IndexSet<WorkerId>,
    pub version: Version,
    pub finished: IndexSet<WorkerId>,
}
impl NormalDistributor {
    pub(super) fn handle_msg<K: DistKey, T: DistData, P: Clone>(
        mut self,
        dist_func: impl Fn(&K, &IndexSet<WorkerId>) -> WorkerId,
        msg: ScalableMessage<K, T>,
        output: &mut Sender<K, T, P>,
        ctx: &mut OperatorContext,
    ) -> PhaseDistributor<K, T> {
        match msg {
            ScalableMessage::Data(m) => {
                if m.version.map_or(false, |v| v > self.version) {
                    output.send(Message::Data(m.message));
                    return PhaseDistributor::Normal(self);
                }
                let target = dist_func(&m.message.key, &self.worker_set);
                if target == ctx.worker_id {
                    output.send(Message::Data(m.message));
                } else {
                    ctx.communication
                        .send(
                            &target,
                            postbox::Message {
                                operator: ctx.operator_id,
                                data: NetworkMessage::Data(m),
                            },
                        )
                        .expect("Remote send Error");
                };
                PhaseDistributor::Normal(self)
            }
            ScalableMessage::ScaleRemoveWorker(set) => {
                let new_set: IndexSet<WorkerId> = self.worker_set.drain(..).filter(|x| !set.contains(x)).collect();
                PhaseDistributor::Interrogate(InterrogateDistributor::from_normal(self, new_set, output))
            }
            ScalableMessage::ScaleAddWorker(set) => {
                let new_set: IndexSet<WorkerId> = self.worker_set.into_iter().chain(set.into_iter()).collect();
                PhaseDistributor::Interrogate(InterrogateDistributor::from_normal(self, new_set, output))
            }
            ScalableMessage::Done(wid) => {
                self.finished.insert(wid);
                PhaseDistributor::Normal(self)
            }
        }
    }
}

pub(super) struct CollectDistributor<K, T> {
    whitelist: IndexSet<K>,
    hold: IndexMap<K, Vec<T>>,
    old_worker_set: IndexSet<WorkerId>,
    new_worker_set: IndexSet<WorkerId>,
    version: Version,
    finished: IndexSet<WorkerId>,
    // if we receive another scale instruction during the interrogation,
    // there is no real good way for us to handle that, so we will
    // queue it up to be handled after collect is done
    queued_rescales: Vec<ScalableMessage<K, T>>,
}
impl<K, T> CollectDistributor<K, T> where K: DistKey, T: DistData{
    pub(super) fn handle_msg<P: Clone>(
        mut self,
        dist_func: impl Fn(&K, &IndexSet<WorkerId>) -> WorkerId,
        msg: ScalableMessage<K, T>,
        output: &mut Sender<K, T, P>,
        ctx: &mut OperatorContext,
    ) -> PhaseDistributor<K, T> {
        match msg {
            ScalableMessage::Data(m) => {todo!()}
            ScalableMessage::ScaleRemoveWorker(set) => {todo!()}
            ScalableMessage::ScaleAddWorker(set) => {todo!()}
            ScalableMessage::Done(wid) => {todo!()}
        }
    }

}

struct InterrogateDistributor<K, T> {
    whitelist: Rc<Mutex<IndexSet<K>>>,
    old_worker_set: IndexSet<WorkerId>,
    new_worker_set: IndexSet<WorkerId>,
    version: Version,
    finished: IndexSet<WorkerId>,
    // if we receive another scale instruction during the interrogation,
    // there is no real good way for us to handle that, so we will
    // queue it up to be handled after collect is done
    queued_rescales: Vec<ScalableMessage<K, T>>,
}
impl<K, T> InterrogateDistributor<K, T>
where
    K: DistKey,
    T: DistData,
{
    fn from_normal<P: Clone>(
        normal: NormalDistributor,
        new_worker_set: IndexSet<WorkerId>,
        output: &mut Sender<K, T, P>,
    ) -> Self {
        let whitelist = Rc::new(Mutex::new(IndexSet::new()));
        let interrogate = Interrogate { shared: whitelist };
        output.send(Message::Interrogate(interrogate));

        Self {
            whitelist: Rc::new(Mutex::new(IndexSet::new())),
            old_worker_set: normal.worker_set,
            new_worker_set,
            version: normal.version,
            finished: normal.finished,
            queued_rescales: Vec::new(),
        }
    }
    fn send_local<P: Clone>(
        &self,
        dist_func: impl Fn(&K, &IndexSet<WorkerId>) -> WorkerId,
        msg: DataMessage<K, T>,
        output: &mut Sender<K, T, P>,
        local_wid: WorkerId,
    ) -> () {
        if (dist_func)(&msg.key, &self.new_worker_set) != local_wid {
            self.whitelist.lock().unwrap().insert(msg.key.clone());
        }
        output.send(Message::Data(msg))
    }

    fn send_remote(
        &self,
        target: &WorkerId,
        msg: DataMessage<K, T>,
        ctx: &mut OperatorContext,
    ) -> () {
        ctx.communication
            .send(
                target,
                postbox::Message {
                    operator: ctx.operator_id,
                    data: NetworkMessage::Data(VersionedMessage {
                        version: Some(self.version),
                        message: msg,
                    }),
                },
            )
            .expect("Network Send Error");
    }

    fn handle_msg<P: Clone>(
        mut self,
        dist_func: impl Fn(&K, &IndexSet<WorkerId>) -> WorkerId,
        msg: ScalableMessage<K, T>,
        output: &mut Sender<K, T, P>,
        ctx: &mut OperatorContext,
    ) -> PhaseDistributor<K, T> {
        match msg {
            ScalableMessage::Data(d) => {
                // if the messages version is greater than our version we send it
                // downstream
                if d.version.map_or(false, |v| v > self.version) {
                    self.send_local(dist_func, d.message, output, ctx.worker_id);
                    return PhaseDistributor::Interrogate(self);
                }
                let old_target = dist_func(&d.message.key, &self.old_worker_set);
                if old_target != ctx.worker_id {
                    self.send_remote(&old_target, d.message, ctx);
                    return PhaseDistributor::Interrogate(self);
                }
                self.send_local(dist_func, d.message, output, ctx.worker_id);
                PhaseDistributor::Interrogate(self)
            }
            ScalableMessage::ScaleRemoveWorker(x) => {
                self.queued_rescales
                    .push(ScalableMessage::ScaleRemoveWorker(x));
                PhaseDistributor::Interrogate(self)
            }
            ScalableMessage::ScaleAddWorker(x) => {
                self.queued_rescales
                    .push(ScalableMessage::ScaleAddWorker(x));
                PhaseDistributor::Interrogate(self)
            }
            ScalableMessage::Done(x) => {
                self.finished.insert(x);
                PhaseDistributor::Interrogate(self)
            }
        }
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
    pub fn add_keys(&mut self, keys: &[K]) -> () {
        let mut guard = self.shared.lock().unwrap();
        for key in keys.into_iter().map(|x| x.clone()) {
            guard.insert(key);
        }
    }
}

#[derive(Debug, Clone)]
pub struct Collect<K> {
    key: K,
    collection: Rc<Mutex<IndexMap<OperatorId, Vec<u8>>>>,
}
impl<K> Collect<K>
where
    K: Key,
{
    pub fn add_state(&mut self, operator_id: OperatorId, state: Vec<u8>) -> () {
        self.collection.lock().unwrap().insert(operator_id, state);
    }
}

#[derive(Debug, Clone)]
pub struct Acquire<K> {
    key: K,
    collection: Rc<Mutex<IndexMap<OperatorId, Vec<u8>>>>,
}
impl<K> Collect<K>
where
    K: Key,
{
    pub fn take_state(&self, operator_id: &OperatorId) -> Option<(K, Vec<u8>)> {
        self.collection
            .lock()
            .unwrap()
            .swap_remove(operator_id)
            .map(|s| (self.key, s))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct NetworkAcquire<K> {
    key: K,
    collection: IndexMap<OperatorId, Vec<u8>>
}

impl<K> From<Acquire<K>> for NetworkAcquire<K>{
    fn from(value: Acquire<K>) -> Self {
        Self { key: value.key, collection: value.collection.into_inner().unwrap() }
    }
}
impl<K> From<NetworkAcquire<K>> for Acquire<K>{
    fn from(value: NetworkAcquire<K>) -> Self {
        Self { key: value.key, collection: Rc::new(Mutex::new(value.collection)) }
    }
}

#[derive(Serialize, Deserialize)]
struct DoneMessage {
    worker_id: WorkerId,
    version: Version,
}
