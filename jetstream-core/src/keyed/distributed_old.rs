use std::{borrow::Borrow, iter, rc::Rc, sync::Mutex};

use derive_new::new;
use indexmap::{IndexMap, IndexSet};
use itertools::Itertools;
use postbox::NetworkMessage;
use tokio::time::Sleep;

use crate::{
    channels::selective_broadcast::{Receiver, Sender},
    snapshot::{Barrier, Load, PersistenceBackend},
    stream::operator::{BuildContext, OperatorContext},
    time::{MaybeTime, Timestamp},
    Data, DataMessage, Key, MaybeData, MaybeKey, Message, OperatorId, RescaleMessage,
    ShutdownMarker, WorkerId,
};

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use self::{
    collect::CollectDistributor,
    dist_trait::DistributorKind,
    interrogate::InterrogateDistributor,
    messages::{Collect, Interrogate, OutgoingMessage, RemoteMessage, VersionedMessage},
    normal::NormalDistributor,
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
pub trait DistData: MaybeData + Serialize + DeserializeOwned {}
impl<T: MaybeData + Serialize + DeserializeOwned> DistData for T {}

pub trait DistTimestamp: MaybeTime + Serialize + DeserializeOwned {}
impl<T: MaybeTime + Serialize + DeserializeOwned> DistTimestamp for T {}

struct Distributor<K, V, T> {
    worker_id: WorkerId,
    inner_dist: DistributorKind<K, V, T>,
    worker_states: IndexMap<WorkerId, WorkerState<T>>,

    partitioner: Rc<dyn WorkerPartitioner<K>>,
    held_barrier: Option<Barrier>,
    // we can not shut down during the rescale, so we must hold onto the marker
    held_shutdown: Option<ShutdownMarker>,
    queued_rescales: Vec<RescaleMessage>,
}

/// Information we have about the remote worker
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
struct WorkerState<T> {
    version: Version,
    // highest epoch they sent us
    latest_epoch: Option<T>,
    // whether or not we received a barrier for alignement from them
    barrier_received: bool,
}
impl<T> Default for WorkerState<T> {
    fn default() -> Self {
        Self {
            version: 0,
            latest_epoch: None,
            barrier_received: false,
        }
    }
}

#[derive(Serialize, Deserialize)]
struct DistributorPersistentState<T> {
    worker_states: IndexMap<WorkerId, WorkerState<T>>,
    dist: NormalDistributor,
    queued_rescales: Vec<RescaleMessage>,
}

impl<T> DistributorPersistentState<T> {
    fn new(worker_id: &WorkerId, peers: Vec<&WorkerId>) -> Self {
        let worker_states: IndexMap<WorkerId, WorkerState<T>> = peers
            .iter()
            .chain(iter::once(&worker_id))
            .map(|x| (**x, WorkerState::default()))
            .collect();
        let worker_set = worker_states.keys().cloned().collect();
        Self {
            worker_states,
            dist: NormalDistributor::new(*worker_id, worker_set),
            queued_rescales: Vec::new(),
        }
    }
}

impl<K, V, T> Distributor<K, V, T>
where
    K: DistKey,
    V: DistData,
    T: DistTimestamp,
{
    fn new(partitioner: impl WorkerPartitioner<K>, ctx: &BuildContext) -> Self {
        let state = ctx.load_state().unwrap_or_else(|| {
            DistributorPersistentState::new(&ctx.worker_id, ctx.communication.get_peers())
        });

        let worker_id = ctx.worker_id;
        let peers = ctx
            .communication
            .get_peers()
            .into_iter()
            .cloned()
            .collect_vec();
        Self {
            worker_id,
            inner_dist: DistributorKind::Normal(state.dist),
            worker_states: state.worker_states,
            partitioner: Rc::new(partitioner),
            held_barrier: None,
            held_shutdown: None,
            queued_rescales: state.queued_rescales,
        }
    }

    /// Handle a data message, received either from local or remote upstream
    fn handle_data(
        &mut self,
        msg: DataMessage<K, V, T>,
        msg_version: Option<Version>,
        msg_sender: WorkerId,
    ) -> OutgoingMessage<K, V, T> {
        let msg = match &mut self.inner_dist {
            DistributorKind::Normal(x) => x.handle_msg(msg, msg_version, self.partitioner.as_ref()),
            DistributorKind::Interrogate(x) => {
                x.handle_msg(msg, msg_version, self.partitioner.as_ref())
            }
            DistributorKind::Collect(ref mut x) => {
                x.handle_msg(msg, msg_version, msg_sender, self.partitioner.as_ref())
            }
        };
        msg
    }

    /// Route a produced message to the correct targed
    fn route_msg(
        &self,
        msg: OutgoingMessage<K, V, T>,
        output: &mut Sender<K, V, T>,
        ctx: &OperatorContext,
    ) {
        match msg {
            OutgoingMessage::Local(l) => output.send(l.into_message()),
            OutgoingMessage::Remote(wid, data) => {
                let rmsg = RemoteMessage::Data(data);
                ctx.communication.send_same(&wid, rmsg).unwrap();
            }
            OutgoingMessage::None => (),
        }
    }

    // // Handle an incoming epoch, either from local or remote
    // fn try_issue_epoch(&mut self, output: &mut Sender<K, V, T>, ctx: &OperatorContext) {
    //     // find the common epoch
    //     let merged = merge_timestamps(self.worker_states.values().map(|x| &x.latest_epoch));
    //     if let Some(m) = merge_timestamps(self.worker_states.values().map(|x| &x.latest_epoch)) {
    //         output.send(Message::Epoch(m))
    //     }
    // }

    fn handle_local_message(
        mut self,
        msg: Message<K, V, T>,
        output: &mut Sender<K, V, T>,
        ctx: &OperatorContext,
    ) -> Self {
        match msg {
            Message::Data(x) => {
                let msg = self.handle_data(x, None, self.worker_id);
                self.route_msg(msg, output, ctx)
            }
            Message::Epoch(e) => {
                // broadcast epoch to all other workers
                ctx.communication.broadcast(RemoteMessage::<K, V, T>::Epoch(e.clone()));
                self.worker_states.get_mut(&ctx.worker_id).unwrap().latest_epoch.insert(e);
                if let Some(m) = merge_timestamps(self.worker_states.values().map(|x| &x.latest_epoch)) {
                    output.send(Message::Epoch(m))
                }
            },
            Message::AbsBarrier(x) => {
                self.held_barrier.replace(x);
                self.worker_states
                    // own id is guaranteed to exist
                    .get_mut(&ctx.worker_id)
                    .unwrap()
                    .barrier_received = true;
                if let Some(x) = self.try_snapshot(ctx) {
                    output.send(Message::AbsBarrier(x))
                }
            }
            Message::Rescale(r) => {
                output.send(Message::Rescale(r.clone()));
                self.inner_dist = match self.inner_dist {
                    DistributorKind::Normal(x) => {
                        DistributorKind::Interrogate(x.into_interrogate(r))
                    }
                    d => {
                        self.queued_rescales.push(r);
                        d
                    }
                };
            }
            Message::ShutdownMarker(x) => match self.inner_dist {
                DistributorKind::Normal(_) => output.send(Message::ShutdownMarker(x)),
                _ => {
                    self.held_shutdown.insert(x);
                }
            },
            Message::Interrogate(_) => (),
            Message::Collect(_) => (),
            Message::Acquire(_) => (),
            Message::DropKey(_) => (),
        };
        self
    }

    fn handle_outgoing(
        &mut self,
        msg: OutgoingMessage<K, V, T>,
        output: &mut Sender<K, V, T>,
        ctx: &OperatorContext,
    ) {
        match msg {
            OutgoingMessage::Local(l) => output.send(l.into_message()),
            OutgoingMessage::Remote(wid, data) => {
                let rmsg = RemoteMessage::Data(data);
                ctx.communication.send_same(&wid, rmsg).unwrap();
            }
            OutgoingMessage::None => (),
        }
    }  

    /// Try making a snapshot of this operator, returning the barrier
    /// if successfull. A snaphsot will only succeed if we are in the Normal
    /// ICADD phase and have received a barrier from all remote workers
    fn try_snapshot(&mut self, ctx: &OperatorContext) -> Option<Barrier> {
        if self.worker_states.values().all(|x| x.barrier_received) {
            // must not be rescaling at the moment
            if let DistributorKind::Normal(d) = &self.inner_dist {
                // and must have a barrier ourselves
                if let Some(mut b) = self.held_barrier.take() {
                    // TODO: This is a bit dumb, we only clone because we can not (safely)
                    // move out of self.
                    let state = DistributorPersistentState {
                        worker_states: self.worker_states.clone(),
                        dist: d.clone(),
                        queued_rescales: self.queued_rescales.clone(),
                    };
                    b.persist(&state, &ctx.operator_id);
                    for x in self.worker_states.values_mut() {
                        x.barrier_received = false
                    }
                    return Some(b);
                }
            }
        };
        None
    }

    /// Handles messages received from other workers
    fn handle_remote_message(
        &mut self,
        sender: WorkerId,
        msg: RemoteMessage<K, V, T>,
        output: &mut Sender<K, V, T>,
        ctx: &OperatorContext,
    ) {
        let partitioner = self.partitioner.as_ref();
        match msg {
            RemoteMessage::Data(VersionedMessage { version, message }) => {
                match &mut self.inner_dist {
                    DistributorKind::Normal(x) => x.handle_msg(message, Some(version), partitioner),
                    DistributorKind::Interrogate(x) => x.handle_msg(message, Some(version), partitioner),
                    DistributorKind::Collect(ref mut x) => {
                        x.handle_msg(message, Some(version), sender, partitioner)
                    }
                };
            }
            RemoteMessage::Epoch(e) => {
                self.worker_states.get_mut(&sender).unwrap().latest_epoch.insert(e);
                if let Some(m) = merge_timestamps(self.worker_states.values().map(|x| &x.latest_epoch)) {
                    output.send(Message::Epoch(m))
                }
            }
            RemoteMessage::Done(d) => {
                self.worker_states.get_mut(&d.worker_id).unwrap().version = d.version;
            }
            RemoteMessage::Acquire(a) => output.send(Message::Acquire(a.into())),
            RemoteMessage::AbsBarrier(b) => {
                println!("Received barrier from {b:?}");
                self.worker_states.get_mut(&b).unwrap().barrier_received = true;
                if let Some(x) = self.try_snapshot(ctx) {
                    output.send(Message::AbsBarrier(x))
                }
            }
            ShutdownMarker => {
                self.worker_states.swap_remove(&sender);
            },
        }
    }

    /// Schedule this distributor to run
    fn run(
        mut self,
        input: &mut Receiver<K, V, T>,
        output: &mut Sender<K, V, T>,
        ctx: &OperatorContext,
    ) -> Self {

        // The interrogate distributor may need to be scheduled to issue its interrogate message
        // The Collect distributor needs to be scheduled to create collector and acquire messages
        match &mut self.inner_dist {
            DistributorKind::Normal(x) => (),
            DistributorKind::Interrogate(x) => {
                let msg = x.run(self.partitioner.clone());
                self.route_msg(msg, output, ctx);
            }
            DistributorKind::Collect(ref mut x) => {
                // coroutines would have been so nice for this
                for msg in x.run(self.partitioner.as_ref()).into_iter() {
                    self.route_msg(msg, output, ctx)
                }
            }
        };

        if let Some(msg) = input.recv() {
            self = self.handle_local_message(msg, output, ctx);
        };
        for x in ctx.communication.recv_all() {
            let sender = x.sender_worker;
            let msg = x.data;
            self.handle_remote_message(sender, msg, output, ctx)
        }

        self
    }
}

/// Small reducer hack, as we can't use iter::reduce because of ownership
fn merge_timestamps<'a, T: MaybeTime>(
    mut timestamps: impl Iterator<Item = &'a Option<T>>,
) -> Option<T> {
    let mut merged = timestamps.next()?.clone();
    for x in timestamps {
        if let Some(y) = x {
            merged = merged.map(|a| a.try_merge(y)).flatten();
        } else {
            return None;
        }
    }
    merged
}

// #[cfg(test)]
// mod test {
//     use std::time::Duration;

//     use indexmap::IndexSet;
//     use postbox::{CommunicationBackend, Postbox, RecvIterator};
//     use test::messages::TargetedMessage;

//     use self::messages::{Acquire, Collect, Interrogate};

//     use super::*;
//     use crate::{
//         channels::selective_broadcast::{self, full_broadcast},
//         snapshot::{NoPersistence, PersistenceBackendBuilder},
//         test::CapturingPersistenceBackend,
//         time::NoTime,
//         NoData, WorkerId,
//     };

//     // a partitioner that just uses the key as a wrapping index
//     fn partiton_index<'a>(i: &usize, s: &'a IndexSet<WorkerId>) -> &'a WorkerId {
//         s.get_index(i % s.len()).unwrap()
//     }

//     /// Test helper to simulate a two Worker cluster with this local
//     /// worker being index 0 and the other being index 1
//     struct TestSetup {
//         dist: Distributor<usize, NoData, i32>,
//         /// Use this to send messages into the distributor
//         local_in: Sender<usize, NoData, i32>,

//         // connected to local_in
//         local_dist_input: Receiver<usize, NoData, i32>,
//         local_dist_output: Sender<usize, NoData, i32>,

//         /// Use this to get local messages out of the distributor
//         local_out: Receiver<usize, NoData, i32>,

//         local_postbox: Postbox<WorkerId, OperatorId>,
//         // used to emulate another worker sending messages
//         remote: Postbox<WorkerId, OperatorId>,

//         // need to keep them around so they don't get dropped
//         backends: (CommunicationBackend, CommunicationBackend),

//         persistence_backend: Box<dyn PersistenceBackend>,
//     }

//     impl Default for TestSetup {
//         fn default() -> Self {
//             let postbox_local_builder = postbox::BackendBuilder::new(
//                 0,
//                 "127.0.0.1:29091".parse().unwrap(),
//                 vec![(1, "http://127.0.0.1:29092".parse().unwrap())],
//                 vec![42],
//                 4096,
//             );
//             let postbox_remote_builder = postbox::BackendBuilder::new(
//                 1,
//                 "127.0.0.1:29092".parse().unwrap(),
//                 vec![(0, "http://127.0.0.1:29091".parse().unwrap())],
//                 vec![42],
//                 4096,
//             );

//             let postbox_local = postbox_local_builder.for_operator(42).unwrap();
//             let postbox_remote = postbox_remote_builder.for_operator(42).unwrap();

//             let backend_local_build =
//                 std::thread::spawn(move || postbox_local_builder.connect().unwrap());
//             let backend_remote_build =
//                 std::thread::spawn(move || postbox_remote_builder.connect().unwrap());

//             let backend_local = backend_local_build.join().unwrap();
//             let backend_remote = backend_remote_build.join().unwrap();

//             let persistence = CapturingPersistenceBackend::default();
//             let buildcontext = BuildContext::new(
//                 0,
//                 42,
//                 persistence.latest(0),
//                 postbox_local,
//             );
//             let dist = Distributor::new(partiton_index, &buildcontext);

//             let mut local_in = selective_broadcast::Sender::new_unlinked(full_broadcast);
//             let mut local_dist_input = selective_broadcast::Receiver::new_unlinked();
//             selective_broadcast::link(&mut local_in, &mut local_dist_input);

//             let mut local_out = selective_broadcast::Receiver::new_unlinked();
//             let mut local_dist_output = selective_broadcast::Sender::new_unlinked(full_broadcast);
//             selective_broadcast::link(&mut local_dist_output, &mut local_out);

//             Self {
//                 dist,
//                 local_in,
//                 local_dist_input,
//                 local_dist_output,
//                 local_out,
//                 local_postbox: buildcontext.communication,
//                 remote: postbox_remote,
//                 backends: (backend_local, backend_remote),
//                 persistence_backend: persistence.latest(0),
//             }
//         }
//     }

//     impl TestSetup {
//         /// Schedule the distributor
//         fn run_distributor(mut self) -> Self {
//             let ctx = OperatorContext {
//                 worker_id: 0,
//                 operator_id: 42,
//                 communication: &self.local_postbox,
//             };

//             // we need to temporarily swap this
//             self.dist = self.dist.run(
//                 &mut self.local_dist_input,
//                 &mut self.local_dist_output,
//                 &ctx,
//             );
//             self
//         }

//         /// Give a message to the distributor as if it where coming from a local upstream
//         fn send_from_local(
//             &mut self,
//             message: Message<usize, NoData, i32>,
//         ) {
//             self.local_in.send(message)
//         }

//         /// Receive a message from the distributor local downstream
//         fn receive_on_local(
//             &mut self,
//         ) -> Option<Message<usize, NoData, i32>> {
//             self.local_out.recv()
//         }

//         /// places a message into the operatorcontext postbox, as if it was sent by a remote
//         fn send_from_remote(&mut self, message: RemoteMessage<usize, NoData, i32>) {
//             self.remote.send_same(&0, message).unwrap();
//         }

//         /// receive all the messages for the remote worker
//         fn receive_on_remote(
//             &mut self,
//         ) -> RecvIterator<WorkerId, OperatorId, RemoteMessage<usize, NoData, i32>> {
//             self.remote.recv_all()
//         }
//     }

//     // It should build from state if available
//     #[test]
//     fn resume_from_state() {
//         let mut backend = CapturingPersistenceBackend::default();

//         let worker_states = IndexMap::from([(
//             1,
//             WorkerState {
//                 version: 42,
//                 latest_epoch: Some(16),
//                 barrier_received: true,
//             },
//         )]);
//         let queued_rescales = vec![RescaleMessage::ScaleAddWorker(IndexSet::from([3]))];

//         let resume_state = DistributorPersistentState {
//             worker_states: worker_states.clone(),
//             dist: NormalDistributor::new(0, IndexSet::from([0, 1])),
//             queued_rescales: queued_rescales.clone(),
//         };
//         let postbox_local_builder = postbox::BackendBuilder::new(
//             0,
//             "127.0.0.1:29091".parse().unwrap(),
//             vec![(1, "http://127.0.0.1:29092".parse().unwrap())],
//             vec![42],
//             4096,
//         );
//         let postbox_local = postbox_local_builder.for_operator(42).unwrap();
//         let ctx = BuildContext::new(0, 42, persistence.latest(0), postbox_local);

//         backend.persist(&resume_state, &42);

//         let dist: Distributor<usize, NoData, i32> =
//             Distributor::new(partiton_index, &ctx);

//         assert_eq!(dist.worker_states, worker_states);
//         assert!(matches!(dist.inner_dist, DistributorKind::Normal(_)));
//         assert_eq!(dist.queued_rescales, queued_rescales);
//     }

//     // it should snapshot its own state
//     #[test]
//     fn snapshot_state() {
//         let mut setup = TestSetup::default();
//         let backend = setup.persistence_backend.clone();

//         setup.send_from_local(Message::AbsBarrier(Barrier::new(backend.clone())));
//         println!("{:?}", setup.remote.get_peers());
//         setup.send_from_remote(RemoteMessage::AbsBarrier(1));
//         std::thread::sleep(Duration::from_secs(3));
//         setup = setup.run_distributor();
//         assert!(backend
//             .load::<DistributorPersistentState<i32>>(&42)
//             .is_some())
//     }

//     // it should start interrogating on an upscale message
//     #[test]
//     fn interrogate_on_upscale() {
//         let mut setup = TestSetup::default();
//         setup.send_from_local(Message::Rescale(RescaleMessage::ScaleAddWorker(
//             IndexSet::from([2]),
//         )));
//         setup = setup.run_distributor();
//         assert!(matches!(
//             setup.dist.inner_dist,
//             DistributorKind::Interrogate(_)
//         ));
//     }

//     // it should start interrogating on an downpscale message
//     #[test]
//     fn interrogate_on_downscale() {
//         let mut setup = TestSetup::default();
//         setup.send_from_local(Message::Rescale(RescaleMessage::ScaleRemoveWorker(
//             IndexSet::from([1]),
//         )));
//         setup = setup.run_distributor();
//         assert!(matches!(
//             setup.dist.inner_dist,
//             DistributorKind::Interrogate(_)
//         ));
//     }

//     // it should hold up a shutdown marker if there is a rescale running
//     #[test]
//     fn holds_shutdownmarker() {
//         // on interrogate
//         let mut setup = TestSetup::default();
//         setup.dist.inner_dist = DistributorKind::Interrogate(InterrogateDistributor::new(
//             0,
//             IndexSet::from([1]),
//             0,
//             RescaleMessage::ScaleAddWorker(IndexSet::from([2])),
//         ));
//         setup.send_from_local(Message::ShutdownMarker(ShutdownMarker::default()));
//         setup = setup.run_distributor();
//         assert!(setup.receive_on_local().is_none());

//         // on collect
//         let mut setup = TestSetup::default();
//         setup.dist.inner_dist = DistributorKind::Collect(CollectDistributor::new(
//             0,
//             IndexSet::from([1]),
//             IndexSet::from([0, 1]),
//             IndexSet::from([0, 1, 2]),
//             0,
//         ));
//         setup.send_from_local(Message::ShutdownMarker(ShutdownMarker::default()));
//         setup = setup.run_distributor();
//         assert!(setup.receive_on_local().is_none());

//         // if rescales are queued
//         let mut setup = TestSetup::default();
//         setup.dist.queued_rescales = vec![RescaleMessage::ScaleAddWorker(IndexSet::from([2]))];
//         setup.send_from_local(Message::ShutdownMarker(ShutdownMarker::default()));
//         setup = setup.run_distributor();
//         assert!(setup.receive_on_local().is_none());
//     }

//     // it should queue up incoming rescales when there is already a rescale running
//     #[test]
//     fn queues_rescales() {
//         // on interrogate
//         let mut setup = TestSetup::default();
//         setup.dist.inner_dist = DistributorKind::Interrogate(InterrogateDistributor::new(
//             0,
//             IndexSet::from([1]),
//             0,
//             RescaleMessage::ScaleAddWorker(IndexSet::from([2])),
//         ));
//         setup.send_from_local(Message::Rescale(RescaleMessage::ScaleAddWorker(
//             IndexSet::from([4]),
//         )));
//         setup = setup.run_distributor();
//         assert!(setup.receive_on_local().is_none());
//         assert!(setup.dist.queued_rescales.len() == 1);

//         // on collect
//         let mut setup = TestSetup::default();
//         setup.dist.inner_dist = DistributorKind::Collect(CollectDistributor::new(
//             0,
//             IndexSet::from([1]),
//             IndexSet::from([0, 1]),
//             IndexSet::from([0, 1, 2]),
//             0,
//         ));
//         setup.send_from_local(Message::Rescale(RescaleMessage::ScaleAddWorker(
//             IndexSet::from([4]),
//         )));
//         setup = setup.run_distributor();
//         assert!(setup.receive_on_local().is_none());
//         assert!(setup.dist.queued_rescales.len() == 1);
//     }

//     // it should not snapshot if there is a rescale running
//     #[test]
//     fn no_snapshot_during_rescale() {
//         // on interrogate
//         let mut setup = TestSetup::default();
//         let backend = setup.persistence_backend.clone();
//         setup.dist.inner_dist = DistributorKind::Interrogate(InterrogateDistributor::new(
//             0,
//             IndexSet::from([1]),
//             0,
//             RescaleMessage::ScaleAddWorker(IndexSet::from([2])),
//         ));
//         setup.send_from_local(Message::AbsBarrier(Barrier::new(backend.clone())));
//         setup = setup.run_distributor();
//         assert!(setup.receive_on_local().is_none());
//         assert!(backend
//             .load::<DistributorPersistentState<usize>>(&42)
//             .is_none());

//         // on collect
//         let mut setup = TestSetup::default();
//         let backend = setup.persistence_backend.clone();

//         setup.dist.inner_dist = DistributorKind::Collect(CollectDistributor::new(
//             0,
//             IndexSet::from([1]),
//             IndexSet::from([0, 1]),
//             IndexSet::from([0, 1, 2]),
//             0,
//         ));
//         setup.send_from_local(Message::AbsBarrier(Barrier::new(backend.clone())));
//         setup = setup.run_distributor();
//         assert!(setup.receive_on_local().is_none());
//         assert!(backend
//             .load::<DistributorPersistentState<usize>>(&42)
//             .is_none());
//     }

//     /// It should forward data messages according to the routing roules
//     #[test]
//     fn route_data_messages() {
//         let mut setup = TestSetup::default();

//         setup.send_from_local(Message::Data(DataMessage {
//             key: 0,
//             value: NoData,
//             timestamp: 69,
//         }));
//         setup = setup.run_distributor();
//         let outmsg = setup.receive_on_local().unwrap();

//         assert!(matches!(
//             outmsg,
//             Message::Data(DataMessage {
//                 key: 0,
//                 value: NoData,
//                 timestamp: 69
//             })
//         ));

//         setup.send_from_local(Message::Data(DataMessage {
//             key: 1,
//             value: NoData,
//             timestamp: 33,
//         }));
//         setup = setup.run_distributor();

//         assert!(setup.receive_on_local().is_none());
//         let outmsg = setup.receive_on_remote().next().unwrap();
//         assert!(matches!(
//             outmsg,
//             RemoteMessage::Data(TargetedMessage {
//                 target: 1,
//                 version: 0,
//                 message: DataMessage {
//                     key: 1,
//                     value: NoData,
//                     timestamp: 33
//                 }
//             })
//         ));
//     }

//     /// It should broadcast epochs and align them
//     #[test]
//     fn broadcast_epochs() {
//         let mut setup = TestSetup::default();
//         setup.send_from_local(Message::Epoch(51));
//         setup = setup.run_distributor();

//         let remote = setup.receive_on_remote().next().unwrap();
//         assert!(matches!(remote, RemoteMessage::Epoch(51)));
//         // should not issue an epoch yet, since it did not receive one from remote
//         assert!(setup.receive_on_local().is_none());

//         setup.send_from_remote(RemoteMessage::Epoch(15));
//         setup = setup.run_distributor();
//         // should now be aligned to 15
//         let local = setup.receive_on_local().unwrap();
//         assert!(matches!(local, Message::Epoch(15)));
//     }

//     // it should align barriers
//     #[test]
//     fn align_barriers() {
//         let mut setup = TestSetup::default();
//         setup.send_from_local(Message::AbsBarrier(Barrier::new(
//             CapturingPersistenceBackend::for_latest(0),
//         )));
//         setup = setup.run_distributor();

//         let remote = setup.receive_on_remote().next().unwrap();
//         assert!(matches!(remote, RemoteMessage::AbsBarrier(0)));
//         // should not issue a barrier yet, since it did not receive one from remote
//         assert!(setup.receive_on_local().is_none());

//         setup.send_from_remote(RemoteMessage::AbsBarrier(1));
//         setup = setup.run_distributor();
//         // should now be aligned
//         let local = setup.receive_on_local().unwrap();
//         assert!(matches!(local, Message::AbsBarrier(_)));

//         // should reset the received epoch state
//         assert!(setup.dist.worker_states.get(&1).unwrap().barrier_received == false)
//     }

//     // it should swallow any incoming key messages
//     #[test]
//     fn swallows_key_messages() {
//         let mut setup = TestSetup::default();
//         let messages: Vec<Message<usize, NoData, i32>> = vec![
//             Message::Interrogate(Interrogate::new(
//                 Rc::new(Mutex::new(IndexSet::new())),
//                 Rc::new(|_: &usize| false),
//             )),
//             Message::Collect(Collect::new(13)),
//             Message::Acquire(Acquire::new(13, Rc::new(Mutex::new(IndexMap::new())))),
//             Message::DropKey(13),
//         ];

//         for m in messages.into_iter() {
//             setup.send_from_local(m);
//             setup = setup.run_distributor();
//             let outlocal = setup.receive_on_local();
//             let outremote = setup.receive_on_remote().next();
//             assert!(outlocal.is_none());
//             assert!(outremote.is_none())
//         }
//     }

//     /// Must forward rescale messages
//     #[test]
//     fn forwards_scalemsg() {
//         todo!()
//     }
// }

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

// struct VersionedMessage<K, V, T> {
//     version: Option<usize>,
//     message: DataMessage<K, V, T>
// }

// struct TargetedMessage<K, V, T> {
//     /// where this message should be routed
//     target: WorkerId,
//     message: VersionedMessage<K, V, T>
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
//     fn try_from_message(value: Message<K, V, T>, sender: &WorkerId, version: &Option<usize>) -> Result<Self, Message<K, V, T>> {
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

// pub(super) struct Distributor<K, V, T> {
//     dist_func: Box<dyn WorkerPartitioner<K>>,
//     worker_versions: IndexMap<WorkerId, Version>,
//     inner: PhaseDistributor<K, V, T>,
//     /// 0 elem is our locally received align, the set is for those received
//     /// from remotes
//     received_barriers: (Option<Barrier>, IndexSet<WorkerId>),
//     received_loads: (Option<Load>, IndexSet<WorkerId>),
// }

// impl<K, V, T> Distributor<K, V, T>
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
//         input: &mut Receiver<K, V, T>,
//         output: &mut Sender<K, V, T>,
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
//         output: &mut Sender<K, V, T>,
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

// fn send_to_target<K: Clone, V: Clone, T: Clone>(
//     msg: VersionedMessage<K, V, T>,
//     target: &WorkerId,
//     output: &mut Sender<K, V, T>,
//     ctx: &OperatorContext,
// ) {
//     if *target == ctx.worker_id {
//         output.send(Message::Data(msg.message))
//     }
// }
