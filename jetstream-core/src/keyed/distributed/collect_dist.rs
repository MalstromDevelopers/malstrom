use std::{collections::VecDeque, rc::Rc};

use indexmap::{IndexMap, IndexSet};
use postbox::{broadcast, Client};

use crate::{
    channels::selective_broadcast::{Receiver, Sender},
    keyed::WorkerPartitioner,
    snapshot::Barrier,
    stream::operator::OperatorContext,
    time::MaybeTime,
    DataMessage, Key, MaybeData, Message, OperatorId, RescaleMessage, ShutdownMarker, WorkerId,
};

use super::{
    icadd_operator::{DirectlyExchangedMessage, DistributorKind, TargetedMessage},
    interrogate_dist::InterrogateDistributor,
    normal_dist::NormalDistributor,
    versioner::VersionedMessage,
    Collect, DistKey, NetworkAcquire, Version,
};

struct RawWhitelist<K>(IndexSet<K>);

enum Whitelist<K> {
    Raw(RawWhitelist<K>),
    Pruned,
}

pub(super) struct CollectDistributor<K, V, T> {
    worker_id: WorkerId,
    whitelist: IndexSet<K>,
    hold: IndexMap<K, Vec<DataMessage<K, V, T>>>,
    old_worker_set: IndexSet<WorkerId>,
    pub(super) new_worker_set: IndexSet<WorkerId>,
    // this contains the union of old and new workers
    clients: IndexMap<WorkerId, Client<DirectlyExchangedMessage<K>>>,
    version: Version,
    // if we receive another scale instruction during the interrogation,
    // there is no real good way for us to handle that, so we will
    // queue it up to be handled after collect is done
    current_collect: Option<Collect<K>>,
    done_workers: IndexSet<WorkerId>,

    // these are control messages we can not handle while rescaling
    // so we will buffer them, waiting for the normal dist to deal with them
    barrier: Option<Barrier>,
    // barrier comes first, then other stuff
    queued_rescales: VecDeque<RescaleMessage>,
    shutdown_marker: Option<ShutdownMarker>,
}

impl<K, V, T> CollectDistributor<K, V, T>
where
    K: DistKey,
    V: MaybeData,
    T: MaybeTime,
{
    pub(super) fn new(
        worker_id: WorkerId,
        whitelist: IndexSet<K>,
        old_worker_set: IndexSet<WorkerId>,
        new_worker_set: IndexSet<WorkerId>,
        version: Version,
        ctx: &mut OperatorContext
    ) -> Self {
        let workers: IndexMap<WorkerId, Client<DirectlyExchangedMessage<K>>> = old_worker_set.union(&new_worker_set).map(|w| (*w, ctx.create_communication_client(*w, ctx.operator_id))).collect();

        Self {
            worker_id,
            whitelist,
            hold: IndexMap::new(),
            old_worker_set,
            new_worker_set,
            clients: workers,
            version: version + 1,
            current_collect: None,
            done_workers: IndexSet::new(),
            barrier: None,
            queued_rescales: VecDeque::new(),
            shutdown_marker: None,
        }
    }

    pub(super) fn try_close(
        mut self,
        output: &mut Sender<K, TargetedMessage<V>, T>,
        ctx: &OperatorContext,
        partitioner: Rc<dyn WorkerPartitioner<K>>,
    ) -> Result<DistributorKind<K, V, T>, Self> {

        if self.whitelist.is_empty() && self.current_collect.is_none() && !self.done_workers.contains(&self.worker_id) {
            broadcast(self.clients.values(), DirectlyExchangedMessage::<K>::Done).unwrap();
            self.done_workers.insert(self.worker_id.clone());
        }

        // will be false for empty diff
        let any_outstanding = self
            .done_workers
            .symmetric_difference(&self.old_worker_set);
        let any_outstanding = self
            .done_workers
            .symmetric_difference(&self.old_worker_set)
            .any(|_| true);
        if self.whitelist.is_empty() && self.current_collect.is_none() && !any_outstanding {
            println!("Closing collector");
            if let Some(trigger) = self.queued_rescales.pop_front() {
                Ok(DistributorKind::Interrogate(
                    InterrogateDistributor::new_from_collect(
                        self.worker_id,
                        self.new_worker_set,
                        self.version,
                        trigger,
                        self.queued_rescales,
                        self.shutdown_marker,
                    ),
                ))
            } else {
                if let Some(x) = self.shutdown_marker {
                    output.send(Message::ShutdownMarker(x));
                }
                let n = &self.new_worker_set;
                println!("Creating normal dist with worker set: {n:?}");
                Ok(NormalDistributor::new_from_collect(
                    self.worker_id,
                    self.new_worker_set,
                    self.barrier,
                    output,
                    ctx,
                    partitioner,
                ))
            }
        } else {
            Err(self)
        }
    }

    fn handle_msg(
        &mut self,
        msg: DataMessage<K, VersionedMessage<V>, T>,
        partitioner: &dyn WorkerPartitioner<K>,
    ) -> Option<DataMessage<K, TargetedMessage<V>, T>> {
        if msg.value.version > self.version {
            // pass downstream
            return Some(DataMessage::new(
                msg.key,
                TargetedMessage::new(msg.value, None),
                msg.timestamp,
            ));
        }

        let new_target = partitioner(&msg.key, &self.new_worker_set);
        let is_in_whitelist = self.whitelist.contains(&msg.key);
        let is_on_hold = self.hold.contains_key(&msg.key);

        let inner = match (*new_target == self.worker_id, is_in_whitelist, is_on_hold) {
            // Rule 1.1.
            (false, true, _) => Some(TargetedMessage::new(
                VersionedMessage::new(msg.value.inner, self.version.clone(), self.worker_id),
                None,
            )),
            // Rule 1.2
            (false, _, true) => {
                self.hold.get_mut(&msg.key).unwrap().push(DataMessage::new(
                    // TODO: Only cloned to satisfy borrow checker
                    msg.key.clone(),
                    msg.value.inner,
                    msg.timestamp.clone(),
                ));
                None
            }
            // Rule 2
            (false, false, false) => Some(TargetedMessage::new(
                VersionedMessage::new(msg.value.inner, self.version.clone(), self.worker_id),
                Some(*new_target),
            )),
            // Rule 3
            (true, _, _) => {
                let old_target = *partitioner(&msg.key, &self.old_worker_set);
                if old_target == msg.value.sender {
                    Some(TargetedMessage::new(
                        VersionedMessage::new(
                            msg.value.inner,
                            self.version.clone(),
                            self.worker_id,
                        ),
                        None,
                    ))
                } else {
                    Some(TargetedMessage::new(
                        VersionedMessage::new(
                            msg.value.inner,
                            self.version.clone(),
                            self.worker_id,
                        ),
                        Some(old_target),
                    ))
                }
            }
        };
        inner.map(|x| DataMessage::new(msg.key, x, msg.timestamp))
    }

    /// We return a vec here, since we want to return Acquire, DropKey, Held messages, all at once
    // pub(super) fn run(
    //     &mut self,
    //     partitioner: &dyn WorkerPartitioner<K>,
    // ) -> Vec<OutgoingMessage<K, V, T>> {
    //     let mut out = Vec::new();
    //     out.extend(self.lifecycle_collector(partitioner));

    //     // // if the whitelist is empty and there is no collect currently being held,
    //     // // we are done
    //     // if self.whitelist.len() == 0 && self.current_collect.is_none() && !self.is_done {
    //     //     out.push(OutgoingMessage::Remote(RemoteMessage::Done(
    //     //         DoneMessage::new(self.worker_id, self.version),
    //     //     )));
    //     //     self.is_done = true;
    //     // };
    //     out
    // }

    pub(super) fn run(
        mut self,
        input: &mut Receiver<K, VersionedMessage<V>, T>,
        output: &mut Sender<K, TargetedMessage<V>, T>,
        ctx: &OperatorContext,
        partitioner: Rc<dyn WorkerPartitioner<K>>,
    ) -> DistributorKind<K, V, T> {
        self.lifecycle_collector(output, partitioner.as_ref(), ctx);

        for (sender, client) in self.clients.iter() {
            for x in client.recv_all().map(|x| x.unwrap()) {
                match x {
                    DirectlyExchangedMessage::Done => {
                        self.done_workers.insert(*sender);
                    }
                    DirectlyExchangedMessage::Acquire(a) => {
                        println!("Got Acquire");
                    
                        output.send(Message::Acquire(a.into()))
                    },
                }
            }
        }

        if self.barrier.is_none() {
            // can't keep receiving messages if barred
            if let Some(msg) = input.recv() {
                match msg {
                    Message::Data(d) => {
                        if let Some(x) = self.handle_msg(d, partitioner.as_ref()) {
                            output.send(Message::Data(x))
                        }
                    }
                    Message::Epoch(e) => output.send(Message::Epoch(e)),
                    Message::AbsBarrier(b) => {
                        self.barrier.replace(b);
                    }
                    Message::Rescale(x) => self.queued_rescales.push_back(x),
                    Message::ShutdownMarker(s) => {
                        self.shutdown_marker.replace(s);
                    }
                    Message::Interrogate(_) => unreachable!(),
                    Message::Collect(_) => unreachable!(),
                    Message::Acquire(_) => unreachable!(),
                    Message::DropKey(_) => unreachable!(),
                }
            }
        }
        match self.try_close(output, ctx, partitioner) {
            Ok(x) => x,
            Err(x) => DistributorKind::Collect(x)
        }
    }

    /// Run the collector lifecycle, i.e. checking if a collector can be unwrapped, because
    /// all operators processed it.
    /// If so unwrap it and create the messages for Acquire, held messages and dropkey, and create
    /// the next collector if possible
    fn lifecycle_collector(
        &mut self,
        output: &mut Sender<K, TargetedMessage<V>, T>,
        partitioner: &dyn WorkerPartitioner<K>,
        ctx: &OperatorContext,
    ) -> () {
        self.current_collect = match self.current_collect.take() {
            Some(x) => match x.try_unwrap() {
                Ok((key, collection)) => {
                    let target = partitioner(&key, &self.new_worker_set);
                    self.clients.get(target).unwrap().send(DirectlyExchangedMessage::Acquire(NetworkAcquire::new(
                        key.clone(),
                        collection,
                    ))).unwrap();
                    println!("Sent Acquire");

                    // send out any held back messages
                    for x in self.hold.swap_remove(&key).unwrap_or_default().into_iter() {
                        output.send(Message::Data(DataMessage::new(
                            x.key,
                            TargetedMessage::new(
                                VersionedMessage::new(x.value, self.version, self.worker_id),
                                Some(*target),
                            ),
                            x.timestamp,
                        )))
                    }
                    // tell downstream they can drop the values for these keys
                    output.send(Message::DropKey(key));
                    self.create_next_collector()
                }
                Err(collector) => Some(collector),
            },
            None => {
                let collector = self.create_next_collector();
                if let Some(c) = collector.as_ref() {
                    output.send(Message::Collect(c.clone()))
                }
                collector
            }
        };
    }

    /// Try to create the next collector and return it,
    /// return None if all keys have been collected
    fn create_next_collector(&mut self) -> Option<Collect<K>> {
        if let Some(k) = self.whitelist.pop() {
            let collector = Collect::new(k);
            Some(collector)
        } else {
            None
        }
    }
}

// #[cfg(test)]
// mod test {
//     use indexmap::{IndexMap, IndexSet};
//     use itertools::Itertools;

//     use crate::{
//         keyed::distributed::messages::{
//             DoneMessage, LocalOutgoingMessage, RemoteMessage, TargetedMessage,
//         },
//         time::NoTime,
//         NoData,
//     };

//     use super::*;
//     // a partitioner that just uses the key as a wrapping index
//     fn partiton_index<'a>(i: &usize, s: &'a IndexSet<WorkerId>) -> &'a WorkerId {
//         s.get_index(i % s.len()).unwrap()
//     }

//     fn make_upscale_distributor() -> CollectDistributor<usize, NoData, NoTime> {
//         CollectDistributor::new(
//             0,
//             IndexSet::from([1]),
//             IndexSet::from([0]),
//             IndexSet::from([0, 1]),
//             0,
//         )
//     }

//     /// Check the stored version gets incremented by one
//     #[test]
//     fn increases_version_on_new() {
//         let dist = make_upscale_distributor();
//         assert_eq!(dist.version, 1)
//     }

//     // /// It must create a complete version map on creation so we can know,
//     // /// when we are done
//     // #[test]
//     // fn creates_version_set_on_new() {
//     //     let dist: CollectDistributor<usize, NoData, NoTime> = CollectDistributor::new(
//     //         0,
//     //         IndexSet::from([]),
//     //         IndexSet::from([0, 1, 2, 3]),
//     //         IndexSet::from([0]),
//     //         0,
//     //         IndexMap::from([(2, 1)]),
//     //         Vec::new(),
//     //     );
//     //     assert_eq!(
//     //         dist.remote_versions,
//     //         IndexMap::from([(1, 0), (2, 1), (3, 0)])
//     //     );
//     // }

//     /// Should respect Rule 1.1
//     /// • Rule 1.1: If (F'(K) != Local) && K ∈ whitelist
//     /// • We will not have the state under the new configuration, but currently it is still located
//     /// here  -> pass downstream
//     #[test]
//     fn handle_data_rule_1_1() {
//         let mut dist = make_upscale_distributor();
//         dist.whitelist.insert(1);
//         let msg = DataMessage {
//             key: 1,
//             value: NoData,
//             timestamp: NoTime,
//         };
//         let msg = dist.handle_msg(msg, None, 0, &partiton_index);
//         assert!(matches!(msg, OutgoingMessage::Local(_)));
//     }

//     /// Should respect Rule 1.2
//     /// Rule 1.2: (F'(K) != Local) && K ∈ hold
//     /// • We will not have the state under the new configuration, but currently it is being collected
//     /// here
//     /// • -> buffer the message
//     #[test]
//     fn handle_data_rule_1_2() {
//         let mut dist = make_upscale_distributor();
//         dist.hold.insert(1, Vec::new());
//         let msg = DataMessage {
//             key: 1,
//             value: NoData,
//             timestamp: NoTime,
//         };
//         let msg = dist.handle_msg(msg, None, 0, &partiton_index);
//         assert!(matches!(msg, OutgoingMessage::None));
//         assert_eq!(dist.hold.get(&1).unwrap().len(), 1);
//     }

//     ///Rule 2: (F'(K) != Local) && K ∉ whitelist && K ∉ hold
//     /// • We do not have state for this key and we will not have
//     ///   it under the new configuration • -> distribute via F'
//     #[test]
//     fn handle_data_rule_2() {
//         let mut dist = make_upscale_distributor();
//         let msg = DataMessage {
//             key: 1,
//             value: NoData,
//             timestamp: NoTime,
//         };
//         let msg = dist.handle_msg(msg, None, 0, &partiton_index);
//         assert!(matches!(
//             msg,
//             OutgoingMessage::Remote(RemoteMessage::Data(TargetedMessage {
//                 target: 1,
//                 version: 1,
//                 message: _
//             }))
//         ))
//     }

//     /// Rule 3
//     /// Rule 3: (F'(K) == Local)
//     /// • if F(K) == Sender: -> pass downstream • else: distribute the message via F
//     #[test]
//     fn handle_data_rule_3() {
//         let mut dist = make_upscale_distributor();
//         let msg = DataMessage {
//             key: 0,
//             value: NoData,
//             timestamp: NoTime,
//         };
//         // should pass downstream since F'(0) == 0 and F(0) == 0 (sender)
//         let msg = dist.handle_msg(msg, None, 0, &partiton_index);
//         assert!(matches!(
//             msg,
//             OutgoingMessage::Local(LocalOutgoingMessage::Data(_))
//         ));
//         let msg = DataMessage {
//             key: 0,
//             value: NoData,
//             timestamp: NoTime,
//         };
//         // should send to 1 since F'(0) == 0 but F(1) == 1
//         let msg = dist.handle_msg(msg, None, 0, &partiton_index);
//         assert!(matches!(
//             msg,
//             OutgoingMessage::Remote(RemoteMessage::Data(TargetedMessage {
//                 target: 1,
//                 version: _,
//                 message: _,
//             }))
//         ));
//     }

//     /// Should create a Collector message and send it downstream locally
//     #[test]
//     fn sends_initial_collector() {
//         let mut dist: CollectDistributor<usize, NoData, NoTime> = CollectDistributor::new(
//             0,
//             IndexSet::from([1, 3]),
//             IndexSet::from([0]),
//             IndexSet::from([0, 1]),
//             0,
//         );
//         let mut msg = dist.run(&partiton_index);

//         assert_eq!(msg.len(), 1);
//         let key = match msg.pop().unwrap() {
//             OutgoingMessage::Local(LocalOutgoingMessage::Collect(c)) => c.key,
//             _ => panic!(),
//         };
//         assert_eq!(key, 3)
//     }

//     /// should send another collector message once the initial one is done
//     #[test]
//     fn sends_next_collector() {
//         let mut dist: CollectDistributor<usize, NoData, NoTime> = CollectDistributor::new(
//             0,
//             IndexSet::from([1, 3]),
//             IndexSet::from([0]),
//             IndexSet::from([0, 1]),
//             0,
//         );
//         let mut msg = dist.run(&partiton_index);
//         let key = match msg.pop().unwrap() {
//             OutgoingMessage::Local(LocalOutgoingMessage::Collect(c)) => c.key,
//             _ => panic!(),
//         };
//         assert_eq!(key, 3);
//         // droping the collector should prompt it to send another one downstream
//         drop(msg);
//         let mut msg = dist.run(&partiton_index);
//         let key = match msg.pop().unwrap() {
//             OutgoingMessage::Local(LocalOutgoingMessage::Collect(c)) => c.key,
//             _ => panic!(),
//         };
//         assert_eq!(key, 1)
//     }

//     /// Should create an Acquire, Drop_key message once the collector is done
//     /// and also send out queued message
//     #[test]
//     fn creates_acquire_and_dropkey_message() {
//         let mut dist: CollectDistributor<usize, NoData, NoTime> = CollectDistributor::new(
//             0,
//             IndexSet::from([1, 3]),
//             IndexSet::from([0]),
//             IndexSet::from([0, 1]),
//             0,
//         );
//         let mut msg = dist.run(&partiton_index);

//         assert_eq!(msg.len(), 1);
//         let collector = match msg.pop().unwrap() {
//             OutgoingMessage::Local(LocalOutgoingMessage::Collect(c)) => c,
//             _ => panic!(),
//         };
//         assert_eq!(collector.key, 3);
//         // we now hold a collector with key, one, so all messages with that key should be held up
//         let msg = dist.handle_msg(
//             DataMessage {
//                 key: 1,
//                 value: NoData,
//                 timestamp: NoTime,
//             },
//             None,
//             0,
//             &partiton_index,
//         );
//         assert!(matches!(msg, OutgoingMessage::None));
//         // now we drop the collector
//         drop(collector);
//         // so running should now give us
//         // - An Acquire(key: 1)
//         // - All held messages for key: 1
//         // - A DropKey(key: 1)

//         let mut messages = dist.run(&partiton_index);
//         assert_eq!(messages.len(), 3);
//         let acquire = messages.remove(0);
//         let held = messages.remove(1);
//         let dropkey = messages.remove(2);

//         match acquire {
//             OutgoingMessage::Remote(RemoteMessage::Acquire(a)) => assert_eq!(a.key, 1),
//             _ => panic!(),
//         }

//         assert!(matches!(
//             held,
//             OutgoingMessage::Remote(RemoteMessage::Data(_))
//         ));
//         assert!(matches!(
//             dropkey,
//             OutgoingMessage::Local(LocalOutgoingMessage::DropKey(1))
//         ));
//     }

//     /// Should send a done message once all state is collected
//     #[test]
//     fn sends_done_message() {
//         let mut dist: CollectDistributor<usize, NoData, NoTime> = CollectDistributor::new(
//             0,
//             IndexSet::from([1]),
//             IndexSet::from([0]),
//             IndexSet::from([0, 1]),
//             0,
//         );
//         let mut msg = dist.run(&partiton_index);

//         assert_eq!(msg.len(), 1);
//         let collector = match msg.pop().unwrap() {
//             OutgoingMessage::Local(LocalOutgoingMessage::Collect(c)) => c,
//             _ => panic!(),
//         };
//         // now we drop the collector
//         drop(collector);
//         // so running should now give us
//         // - An Acquire(key: 1)
//         // - All held messages for key: 1
//         // - A DropKey(key: 1)
//         // - A DoneMessage
//         let mut messages = dist.run(&partiton_index);
//         assert_eq!(messages.len(), 4);
//         let done = messages.remove(3);

//         assert!(matches!(
//             done,
//             OutgoingMessage::Remote(RemoteMessage::Done(DoneMessage {
//                 worker_id: 0,
//                 version: 1,
//             }))
//         ));

//         // it should send the done message only onve
//         let out = dist.run(&partiton_index);
//         assert!(out.len() == 0);
//     }
// }
