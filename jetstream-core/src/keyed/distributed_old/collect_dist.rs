use std::{collections::VecDeque, rc::Rc};

use indexmap::{IndexMap, IndexSet};

use crate::{
    channels::selective_broadcast::{Receiver, Sender}, keyed::types::{DistKey, WorkerPartitioner}, runtime::{communication::broadcast, CommunicationClient}, snapshot::Barrier, types::{DataMessage, MaybeData, Message, RescaleMessage, ShutdownMarker, Timestamp, WorkerId}
};
use crate::stream::OperatorContext;

use super::{
    icadd_operator::{DirectlyExchangedMessage, DistributorKind, TargetedMessage},
    interrogate_dist::InterrogateDistributor,
    normal_dist::NormalDistributor,
    versioner::VersionedMessage,
    Collect, NetworkAcquire, Version,
};

pub(super) struct CollectDistributor<K, V, T> {
    worker_id: WorkerId,
    whitelist: IndexSet<K>,
    hold: IndexMap<K, Vec<DataMessage<K, V, T>>>,
    old_worker_set: IndexSet<WorkerId>,
    pub(super) new_worker_set: IndexSet<WorkerId>,
    // this contains the union of old and new workers
    clients: IndexMap<WorkerId, CommunicationClient<DirectlyExchangedMessage<K>>>,
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
    T: Timestamp,
{
    pub(super) fn new(
        whitelist: IndexSet<K>,
        old_worker_set: IndexSet<WorkerId>,
        new_worker_set: IndexSet<WorkerId>,
        version: Version,
        ctx: &mut OperatorContext,
    ) -> Self {
        let workers: IndexMap<WorkerId, CommunicationClient<DirectlyExchangedMessage<K>>> =
            old_worker_set
                .union(&new_worker_set)
                .filter_map(|w| 
                    (*w != ctx.worker_id).then_some(
                        (*w, ctx.create_communication_client(*w, ctx.operator_id))
                    )
                    )
                .collect();

        Self {
            worker_id: ctx.worker_id,
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
        if self.whitelist.is_empty()
            && self.current_collect.is_none()
            && !self.done_workers.contains(&self.worker_id)
        {
            broadcast(self.clients.values(), DirectlyExchangedMessage::<K>::Done);
            self.done_workers.insert(self.worker_id);
        }

        // will be false for empty diff
        let any_outstanding = self
            .done_workers
            .symmetric_difference(&self.old_worker_set)
            .any(|_| true);
        if self.whitelist.is_empty() && self.current_collect.is_none() && !any_outstanding {
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
                VersionedMessage::new(msg.value.inner, self.version, self.worker_id),
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
                VersionedMessage::new(msg.value.inner, self.version, self.worker_id),
                Some(*new_target),
            )),
            // Rule 3
            (true, _, _) => {
                let old_target = *partitioner(&msg.key, &self.old_worker_set);
                if old_target == msg.value.sender {
                    Some(TargetedMessage::new(
                        VersionedMessage::new(msg.value.inner, self.version, self.worker_id),
                        None,
                    ))
                } else {
                    Some(TargetedMessage::new(
                        VersionedMessage::new(msg.value.inner, self.version, self.worker_id),
                        Some(old_target),
                    ))
                }
            }
        };
        inner.map(|x| DataMessage::new(msg.key, x, msg.timestamp))
    }

    pub(super) fn run(
        mut self,
        input: &mut Receiver<K, VersionedMessage<V>, T>,
        output: &mut Sender<K, TargetedMessage<V>, T>,
        ctx: &OperatorContext,
        partitioner: Rc<dyn WorkerPartitioner<K>>,
    ) -> DistributorKind<K, V, T> {
        self.lifecycle_collector(output, partitioner.as_ref(), ctx);

        for (sender, client) in self.clients.iter() {
            for x in client.recv_all() {
                match x {
                    DirectlyExchangedMessage::Done => {
                        self.done_workers.insert(*sender);
                    }
                    DirectlyExchangedMessage::Acquire(a) => {
                        println!("Got Acquire");

                        output.send(Message::Acquire(a.into()))
                    }
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
            Err(x) => DistributorKind::Collect(x),
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
        _ctx: &OperatorContext,
    ) {
        self.current_collect = match self.current_collect.take() {
            Some(x) => match x.try_unwrap() {
                Ok((key, collection)) => {
                    let target = partitioner(&key, &self.new_worker_set);
                    self.clients
                        .get(target)
                        .unwrap()
                        .send(DirectlyExchangedMessage::Acquire(NetworkAcquire::new(
                            key.clone(),
                            collection,
                        )));

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
            self.hold.insert(k.clone(), Vec::new());
            let collector = Collect::new(k);
            Some(collector)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod test {
    use indexmap::{IndexSet};
    use crate::{keyed::distributed::icadd_operator::make_icadd_with_dist, runtime::CommunicationBackend, testing::{NoCommunication, OperatorTester, SentMessage}, types::*};

    use super::*;
    // a partitioner that just uses the key as a wrapping index
    fn partiton_index<'a>(i: &usize, s: &'a IndexSet<WorkerId>) -> &'a WorkerId {
        s.get_index(i % s.len()).unwrap()
    }

    fn make_op_ctx(communication_backend: &mut dyn CommunicationBackend) -> OperatorContext {
        OperatorContext::new(0, 5, communication_backend)
    }

    /// Get a tester for a transition from workerset {0} to  {0, 1}
    /// with whitelisted keys {1, 3}
    fn get_upscale_tester(
    ) -> OperatorTester<usize, VersionedMessage<i32>, i32, usize, TargetedMessage<i32>, i32, DirectlyExchangedMessage<usize>>
    {
        OperatorTester::built_by(
            |ctx| {
                let mut op_ctx = make_op_ctx(ctx.communication);
                make_icadd_with_dist(
                    Rc::new(partiton_index),
                    DistributorKind::Collect(CollectDistributor::new(IndexSet::from([1, 3]), IndexSet::from([0]), IndexSet::from([0, 1]), 0, &mut op_ctx)),
                )
            },
            0,
            5,
            0..2,
        )
    }
    /// Get a tester for a transition from workerset {0, 1} to  {0}
    /// with whitelisted keys {1, 3}
    fn get_downscale_tester(
    ) -> OperatorTester<usize, VersionedMessage<i32>, i32, usize, TargetedMessage<i32>, i32, DirectlyExchangedMessage<usize>>
    {
        OperatorTester::built_by(
            |_ctx| {
                let mut comm = NoCommunication::default();
                let mut op_ctx = make_op_ctx(&mut comm);
                make_icadd_with_dist(
                    Rc::new(partiton_index),
                    DistributorKind::Collect(CollectDistributor::new(IndexSet::from([1, 3]), IndexSet::from([0, 1]), IndexSet::from([0]), 0, &mut op_ctx)),
                )
            },
            0,
            5,
            0..2,
        )
    }
    /// Check the stored version gets incremented by one
    #[test]
    fn increases_version_on_new() {
        let mut comm = NoCommunication::default();
        let dist: CollectDistributor<i32, NoData, i32> = CollectDistributor::new(
            IndexSet::from([]),
            IndexSet::from([0, 1, 2, 3]),
            IndexSet::from([0]),
            0,
            &mut make_op_ctx(&mut comm)
        );
        assert_eq!(dist.version, 1)
    }

    /// Should respect Rule 1.1
    /// • Rule 1.1: If (F'(K) != Local) && K ∈ whitelist
    /// • We will not have the state under the new configuration, but currently it is still located
    /// here  -> pass downstream
    #[test]
    fn handle_data_rule_1_1() {
        let mut dist = get_upscale_tester();
        let msg = DataMessage {
            key: 3,
            value: VersionedMessage{inner: 42, version: 0, sender: 0},
            timestamp: 512,
        };
        dist.send_local(Message::Data(msg));
        dist.step();
        // this is the collector
        let _  = dist.recv_local().unwrap();
        dist.step();
        assert!(matches!(dist.recv_local().unwrap(), Message::Data(DataMessage { key: 3, value: _, timestamp: _ })));
    }

    /// Should respect Rule 1.2
    /// Rule 1.2: (F'(K) != Local) && K ∈ hold
    /// • We will not have the state under the new configuration, but currently it is being collected
    /// here
    /// • -> buffer the message
    #[test]
    fn handle_data_rule_1_2() {
        let mut dist = get_upscale_tester();
        let msg = DataMessage {
            key: 3,
            value: VersionedMessage{inner: 42, version: 0, sender: 0},
            timestamp: 512,
        };
        dist.step();
        // hold onto this, so we are collecting the key 3 right now
        let collector  = dist.recv_local().unwrap();
        match &collector {
            Message::Collect(c) => assert_eq!(c.key, 3),
            _ => panic!()
        }
        dist.send_local(Message::Data(msg));
        dist.step();
        // should buffer and not emit data
        let out = dist.recv_local();
        assert!(out.is_none(), "{out:?}");
    }

    /// Rule 2: (F'(K) != Local) && K ∉ whitelist && K ∉ hold
    /// • We do not have state for this key and we will not have
    ///   it under the new configuration • -> distribute via F'
    #[test]
    fn handle_data_rule_2() {
        let mut dist = get_upscale_tester();
        let msg = DataMessage {
            key: 7,
            value: VersionedMessage{inner: 42, version: 0, sender: 0},
            timestamp: 512,
        };
        dist.send_local(Message::Data(msg));
        dist.step();
        // this is the collector
        let _  = dist.recv_local().unwrap();
        dist.step();
        assert!(matches!(dist.recv_local().unwrap(), Message::Data(DataMessage { key: 7, value: TargetedMessage { inner: _, target: Some(1) }, timestamp: _ })));
    }

    /// Rule 3
    /// Rule 3: (F'(K) == Local)
    /// • if F(K) == Sender: -> pass downstream • else: distribute the message via F
    #[test]
    fn handle_data_rule_3() {
        let mut dist = get_downscale_tester();
        let msg = DataMessage {
            key: 7, // F(3) == Remote && F'(3) == Local
            // since sender == 1 == F(3) this should just pass downstream
            value: VersionedMessage{inner: 42, version: 0, sender: 1},
            timestamp: 512,
        };
        dist.step();
        // this is the collector
        let collector  = dist.recv_local().unwrap();

        dist.send_local(Message::Data(msg));
        dist.step();
        assert!(matches!(dist.recv_local().unwrap(), Message::Data(DataMessage { key: 3, value: TargetedMessage { inner: _, target: None }, timestamp: _ })));
        
        // next collector
        drop(collector);
        dist.step();
        let collector = dist.recv_local();

        let msg = DataMessage {
            key: 7, // F(3) == Remote && F'(3) == Local
            // since sender == 0 != F(3) this should go to remote
            value: VersionedMessage{inner: 42, version: 0, sender: 0},
            timestamp: 512,
        };
        dist.send_local(Message::Data(msg));
        dist.step();
        assert!(matches!(dist.recv_local().unwrap(), Message::Data(DataMessage { key: 3, value: TargetedMessage { inner: _, target: Some(1) }, timestamp: _ })));
    }

    /// Should create a Collector message and send it downstream locally
    #[test]
    fn sends_initial_collector() {
        let mut dist = get_upscale_tester();
        dist.step();
        // this is the collector
        let collector  = dist.recv_local().unwrap();
        match collector {
            Message::Collect(c) => assert_eq!(c.key, 1),
            _ => panic!()
        }
    }

    /// should send another collector message once the initial one is done
    #[test]
    fn sends_next_collector() {
        let mut dist = get_upscale_tester();
        dist.step();
        // this is the collector
        let collector  = dist.recv_local().unwrap();
        match collector {
            Message::Collect(c) => assert_eq!(c.key, 1),
            _ => panic!()
        }
        // collector gets dropped here

        dist.step();
        // this is the collector
        let collector  = dist.recv_local().unwrap();
        match collector {
            Message::Collect(c) => assert_eq!(c.key, 3),
            _ => panic!()
        }
    }

    /// Should create an Acquire, Drop_key message once the collector is done
    /// and also send out queued message
    #[test]
    fn creates_acquire_and_dropkey_message() {
        let mut dist = get_upscale_tester();
        dist.step();
        // this is the collector
        let collector  = dist.recv_local().unwrap();
        drop(collector);
        dist.step();
        assert!(matches!(dist.recv_local().unwrap(), Message::DropKey(1)));
        match dist.remote().recv_from_operator().unwrap(){
            SentMessage{worker_id: 1, operator_id: 5, msg: DirectlyExchangedMessage::Acquire(x)} => assert_eq!(x.key, 1),
            _ => panic!()
        }

        let collector  = dist.recv_local().unwrap();
        drop(collector);
        dist.step();
        assert!(matches!(dist.recv_local().unwrap(), Message::DropKey(3)));
        match dist.remote().recv_from_operator().unwrap(){
            SentMessage{worker_id: 1, operator_id: 5, msg: DirectlyExchangedMessage::Acquire(x)} => assert_eq!(x.key, 3),
            _ => panic!()
        }
    }

    /// Should send a done message once all state is collected
    #[test]
    fn sends_done_message() {
        let mut dist = get_upscale_tester();
        dist.step();
        // this is the collector
        let collector  = dist.recv_local().unwrap();
        drop(collector);
        dist.step();

        let collector  = dist.recv_local().unwrap();
        drop(collector);
        dist.step();

        let _ = dist.remote().recv_from_operator(); // Acquire for first key
        let _ = dist.remote().recv_from_operator(); // Acquire for next key
        let done = dist.remote().recv_from_operator().unwrap();
        assert!(matches!(done, SentMessage{worker_id: 1, operator_id: 5, msg: DirectlyExchangedMessage::Done }), "{done:?}")
    }
}
