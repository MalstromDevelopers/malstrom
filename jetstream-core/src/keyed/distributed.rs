use std::{rc::Rc, sync::Mutex};

use derive_new::new;
use indexmap::{IndexMap, IndexSet};

use crate::{
    channels::selective_broadcast::{Receiver, Sender},
    snapshot::{Barrier, Load, PersistenceBackend},
    stream::operator::OperatorContext,
    time::{Epoch, Timestamp},
    Data, DataMessage, Key, Message, OperatorId, ShutdownMarker, WorkerId,
};

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use self::{
    collect::CollectDistributor, dist_trait::DistributorKind, interrogate::InterrogateDistributor, messages::{OutgoingMessage, RemoteMessage}, normal::NormalDistributor
};

use super::WorkerPartitioner;
mod collect;
mod dist_trait;
mod interrogate;
pub mod messages;
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

struct Distributor<K, V, T, P> {
    worker_id: WorkerId,
    inner_dist: DistributorKind<K, V, T>,
    remote_worker_versions: IndexMap<WorkerId, Version>,
    self_version: Version,
    target_version: Version,

}

impl <K, V, T, P> Distributor<K, V, T, P> where K: Key {
    
    fn run(&mut self) {
        match self.inner_dist {
            DistributorKind::Collect(x) => {
                for msg in x.run() {
                    if matches!(msg, OutgoingMessage::Remote(RemoteMessage::Done(_))) {
                        self.self_version = self.target_version;
                    }
                }
            }
        }
    }
}

// / Enum which contains either data messages or instructions to change
// / the cluster scale
// /
// / This exists because are the only message types our phase distributors need to handle
// enum ScalableMessage<K, V, T> {
//     Data(VersionedMessage<K, V, T>),
//     /// Information that the worker of this ID will soon be removed
//     /// from the computation. Triggers Rescaling procedure
//     ScaleRemoveWorker(IndexSet<WorkerId>),
//     /// Information that the worker of this ID will be added to the
//     /// computation. Triggers Rescaling procedure
//     ScaleAddWorker(IndexSet<WorkerId>),
//     /// Sent by remote worker to indicate they are don with the current migration
//     Done(WorkerId),
// }

// struct VersionedMessage<K, V, T, P> {
//     version: Option<usize>,
//     message: DataMessage<K, V, T>
// }

// struct TargetedMessage<K, V, T, P> {
//     /// where this message should be routed
//     target: WorkerId,
//     message: VersionedMessage<K, V, T, P>
// }
// /// Almost the same as the jetstream message, but without the persistence
// /// backend and messages which are local only
// #[derive(Serialize, Deserialize)]
// enum NetworkMessage<K, V, T> {
//     Data(VersionedMessage<K, V, T>),
//     Epoch(T),
//     ScaleRemoveWorker(IndexSet<usize>),
//     ScaleAddWorker(IndexSet<usize>),
//     BarrierAlign(WorkerId),
//     Acquire(NetworkAcquire<K>),
//     Done(WorkerId),
// }

// impl<K, V, T> NetworkMessage<K, V, T> where K: Serialize + DeserializeOwned, V: Serialize + DeserializeOwned, T: Serialize + DeserializeOwned {
//     fn try_from_message<P>(value: Message<K, V, T, P>, sender: &WorkerId, version: &Option<usize>) -> Result<Self, Message<K, V, T, P>> {
//         match value {
//             Message::Data(message) => Ok(NetworkMessage::Data(VersionedMessage::new(version, sender, message))),
//             Message::Epoch(e) => Ok(NetworkMessage::Epoch(e.timestamp)),
//             Message::AbsBarrier(_) => Ok(NetworkMessage::BarrierAlign(sender)),
//             Message::ScaleRemoveWorker(x) => Ok(NetworkMessage::ScaleRemoveWorker(x)),
//             Message::ScaleAddWorker(x) => Ok(NetworkMessage::ScaleAddWorker(x)),
//             Message::Acquire(a) => Ok(NetworkMessage::Acquire(NetworkAcquire::from(a))),
//             x => Err(x)
//         }
//     }
// }

// /// Enum of value distributors for different ICADD
// /// phases
// enum PhaseDistributor<K, V, T> {
//     Normal(NormalDistributor),
//     Interrogate(InterrogateDistributor<K, V, T>),
//     Collect(CollectDistributor<K, V, T>),
//     None,
// }

// pub(super) struct Distributor<K, V, T, P> {
//     dist_func: Box<dyn WorkerPartitioner<K>>,
//     worker_versions: IndexMap<WorkerId, Version>,
//     inner: PhaseDistributor<K, V, T>,
//     /// 0 elem is our locally received align, the set is for those received
//     /// from remotes
//     received_barriers: (Option<Barrier<P>>, IndexSet<WorkerId>),
//     received_loads: (Option<Load<P>>, IndexSet<WorkerId>),
// }

// impl<K, V, T, P> Distributor<K, V, T, P>
// where
//     K: DistKey,
//     V: DistData,
//     T: DistTimestamp,
//     P: PersistenceBackend,
// {
//     pub(super) fn new(dist_func: impl WorkerPartitioner<K>) -> Self {
//         let inner = PhaseDistributor::Normal(NormalDistributor::default());
//         Self {
//             dist_func: Box::new(dist_func),
//             worker_versions: IndexMap::new(),
//             inner,
//             received_barriers: (None, IndexSet::new()),
//             received_loads: (None, IndexSet::new()),
//         }
//     }
//     pub(super) fn run(
//         &mut self,
//         input: &mut Receiver<K, V, T, P>,
//         output: &mut Sender<K, V, T, P>,
//         ctx: &mut OperatorContext,
//     ) {
//         // TODO: check what if any state we need to persist
//         if let Some(msg) = input.recv() {
//             let scalable_message  = match msg {
//                 Message::AbsBarrier(b) => {
//                     let _ = self.received_barriers.0.insert(b);
//                     ctx.communication
//                         .broadcast(NetworkMessage::<K, V, T>::BarrierAlign(ctx.worker_id))
//                         .expect("Communication error");
//                     None
//                 },
//                 Message::Load(p) => {
//                     let _ = self.received_loads.0.insert(p);
//                     ctx.communication
//                         .broadcast(NetworkMessage::<K, V, T>::LoadAlign(ctx.worker_id))
//                         .expect("Communication error");
//                     None
//                 },
//                 Message::Data(d) => Some(ScalableMessage::Data(VersionedMessage{
//                     sender: ctx.worker_id,
//                     version: None,
//                     message: d,
//                 })),
//                 Message::ScaleAddWorker(x) => Some(ScalableMessage::ScaleAddWorker(x)),
//                 Message::ScaleRemoveWorker(x) =>Some( ScalableMessage::ScaleRemoveWorker(x)),
//                 Message::Epoch(_) => todo!(),
//                 Message::ShutdownMarker(_) => todo!(),
//                 Message::Interrogate(_) => todo!(),
//                 Message::Collect(_) => todo!(),
//                 Message::Acquire(_) => todo!(),
//                 Message::DropKey(_) => todo!(),

//             }
//         }

//         // let scalable_message = match input.recv() {

//         //     // simply ignore all keying related messages, since they may not cross here
//         // };
//         self.call_distributor(scalable_message, output, ctx);

//         for remote_message in ctx.communication.recv_all::<NetworkMessage<K, V, T>>() {
//             match remote_message {
//                 NetworkMessage::BarrierAlign(x) => {
//                     self.received_barriers.1.insert(x);
//                 }
//                 NetworkMessage::LoadAlign(x) => {
//                     self.received_loads.1.insert(x);
//                 }
//                 NetworkMessage::Data(x) => {
//                     self.call_distributor(Some(ScalableMessage::Data(x)), output, ctx);
//                 }
//                 NetworkMessage::Acquire(x) => output.send(Message::Acquire(x.into())),
//                 NetworkMessage::Done(x) => {
//                     self.call_distributor(Some(ScalableMessage::Done(x)), output, ctx);
//                 }
//             }
//         }
//         // synchronize barriers and loads
//         if self.received_barriers.1.len() >= ctx.communication.get_peers().len() {
//             if let Some(p) = self.received_barriers.0.take() {
//                 output.send(Message::AbsBarrier(p));
//                 self.received_barriers.1.drain(..);
//             }
//         }
//         if self.received_loads.1.len() >= ctx.communication.get_peers().len() {
//             if let Some(p) = self.received_loads.0.take() {
//                 output.send(Message::Load(p));
//                 self.received_loads.1.drain(..);
//             }
//         }
//     }

//     fn call_distributor(
//         &mut self,
//         msg: Option<ScalableMessage<K, V, T>>,
//         output: &mut Sender<K, V, T, P>,
//         ctx: &mut OperatorContext,
//     ) {
//         let inner = std::mem::replace(&mut self.inner, PhaseDistributor::None);
//         self.inner = match inner {
//             PhaseDistributor::Normal(x) => x.run(&self.dist_func, msg, output, ctx),
//             PhaseDistributor::Interrogate(x) => x.run(&self.dist_func, msg, output, ctx),
//             PhaseDistributor::Collect(x) => x.run(&self.dist_func, msg, output, ctx),
//             PhaseDistributor::None => {
//                 unreachable!("Invariant broken: Inner distributor can not be None")
//             }
//         }
//     }
// }

// fn send_to_target<K: Clone, V: Clone, T: Clone, P>(
//     msg: VersionedMessage<K, V, T>,
//     target: &WorkerId,
//     output: &mut Sender<K, V, T, P>,
//     ctx: &OperatorContext,
// ) {
//     if *target == ctx.worker_id {
//         output.send(Message::Data(msg.message))
//     }
// }
