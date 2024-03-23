use indexmap::{IndexMap, IndexSet};

use crate::{
    keyed::{
        distributed::messages::{NetworkAcquire, RemoteMessage, TargetedMessage},
        WorkerPartitioner,
    }, DataMessage, Key, OperatorId, ShutdownMarker, WorkerId
};

use super::{
    dist_trait::{Distributor, DistributorKind},
    messages::{
        Acquire, Collect, DoneMessage, LocalOutgoingMessage, OutgoingMessage, RescaleMessage
    },
    normal::NormalDistributor,
    Version,
};

struct RawWhitelist<K>(IndexSet<K>);

enum Whitelist<K> {
    Raw(RawWhitelist<K>),
    Pruned,
}

#[derive(Debug)]
pub(super) struct CollectDistributor<K, V, T> {
    worker_id: WorkerId,
    whitelist: IndexSet<K>,
    hold: IndexMap<K, Vec<DataMessage<K, V, T>>>,
    old_worker_set: IndexSet<WorkerId>,
    new_worker_set: IndexSet<WorkerId>,
    version: Version,
    // if we receive another scale instruction during the interrogation,
    // there is no real good way for us to handle that, so we will
    // queue it up to be handled after collect is done
    current_collect: Option<Collect<K>>,
}

impl<K, V, T> CollectDistributor<K, V, T>
where
    K: Key,
{
    pub(super) fn new(
        worker_id: WorkerId,
        whitelist: IndexSet<K>,
        old_worker_set: IndexSet<WorkerId>,
        new_worker_set: IndexSet<WorkerId>,
        version: Version,
        mut remote_versions: IndexMap<WorkerId, Version>,
        queued_rescales: Vec<RescaleMessage>,
    ) -> Self {
        for wid in old_worker_set.iter().filter(|wid| **wid != worker_id) {
            remote_versions.entry(*wid).or_insert(0);
        }
        Self {
            worker_id,
            whitelist,
            hold: IndexMap::new(),
            old_worker_set,
            new_worker_set,
            version: version + 1,
            current_collect: None,
        }
    }

    fn handle_msg(
        &mut self,
        msg: DataMessage<K, V, T>,
        msg_version: Option<Version>,
        sender: WorkerId,
        partitioner: impl crate::keyed::WorkerPartitioner<K>,
    ) -> OutgoingMessage<K, V, T> {
        if matches!(msg_version.map(|x| x > self.version), Some(true)) {
            return OutgoingMessage::Local(LocalOutgoingMessage::Data(msg));
        }
        let new_target = partitioner(&msg.key, &self.new_worker_set);
        let is_in_whitelist = self.whitelist.contains(&msg.key);
        let is_on_hold = self.hold.contains_key(&msg.key);

        match (*new_target == self.worker_id, is_in_whitelist, is_on_hold) {
            // Rule 1.1.
            (false, true, _) => OutgoingMessage::Local(LocalOutgoingMessage::Data(msg)),
            // Rule 1.2
            (false, _, true) => {self.hold.get_mut(&msg.key).unwrap().push(msg); OutgoingMessage::None},
            // Rule 2
            (false, false, false) => OutgoingMessage::Remote(RemoteMessage::Data(TargetedMessage { target: *new_target, version: self.version, message: msg })),
            // Rule 3
            (true, _, _) => {
                let old_target = *partitioner(&msg.key, &self.old_worker_set);
                if old_target == sender {
                    OutgoingMessage::Local(LocalOutgoingMessage::Data(msg))
                } else {
                    OutgoingMessage::Remote(RemoteMessage::Data(TargetedMessage { target: old_target, version: self.version, message: msg }))
                }
            }
        }
    
    }

    /// We return a vec here, since we want to return Acquire, DropKey, Held messages, all at once
    pub(super) fn run(
        &mut self,
        partitioner: impl WorkerPartitioner<K>,
    ) -> Vec<OutgoingMessage<K, V, T>> {
        let mut out = Vec::new();
        out.extend(self.lifecycle_collector(partitioner));

        // if the whitelist is empty and there is no collect currently being held,
        // we are done
        let dist = if self.whitelist.len() == 0 && self.current_collect.is_none() {
            out.push(OutgoingMessage::Remote(RemoteMessage::Done(
                DoneMessage::new(self.worker_id, self.version),
            )));
        };
        out
    }

    /// Run the collector lifecycle, i.e. checking if a collector can be unwrapped, because
    /// all operators processed it.
    /// If so unwrap it and create the messages for Acquire, held messages and dropkey, and create
    /// the next collector if possible
    fn lifecycle_collector(
        &mut self,
        partitioner: impl WorkerPartitioner<K>,
    ) -> Vec<OutgoingMessage<K, V, T>> {
        let mut out = Vec::new();
        self.current_collect = match self.current_collect.take() {
            Some(x) => {
                match x.try_unwrap() {
                    Ok((key, collection)) => {
                        let (collector, messages) = self.package_collector(key, collection, partitioner);
                        out.extend(messages);
                        collector
                    }
                    Err(collector) => Some(collector),
                }
            }
            None => {
                let collector = self.create_next_collector();
                if let Some(c) = collector.as_ref() {
                    out.push(OutgoingMessage::Local(LocalOutgoingMessage::Collect(
                        c.clone(),
                    )))
                };
                collector
            }
        };
        out
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

    /// Package up a finished collector returning the next one, if necessary and
    /// the Acquire, held messages and Dropkey message
    fn package_collector(&mut self, key: K, collection: IndexMap<OperatorId, Vec<u8>>, partitioner: impl WorkerPartitioner<K>) -> (Option<Collect<K>>, Vec<OutgoingMessage<K, V, T>>) {
        let mut out = Vec::new();
        // Collection for this key is done, pack it up
        let acquire = OutgoingMessage::Remote(RemoteMessage::Acquire(
            NetworkAcquire::new(key.clone(), collection),
        ));
        let target = partitioner(&key, &self.new_worker_set);
        let held =
            self.hold
                .swap_remove(&key)
                .unwrap_or_default()
                .into_iter()
                .map(|x| {
                    OutgoingMessage::Remote(RemoteMessage::Data(
                        TargetedMessage::new(*target, self.version, x),
                    ))
                });
        let dropkey = OutgoingMessage::Local(LocalOutgoingMessage::DropKey(key));

        out.push(acquire);
        out.extend(held);
        out.push(dropkey);

        // try to create a new collector
        let collector = self.create_next_collector();
        if let Some(col) = collector.as_ref() {
            out.push(OutgoingMessage::Local(LocalOutgoingMessage::Collect(
                col.clone(),
            )));
        };
        (collector, out)
    }
}

#[cfg(test)]
mod test {
    use indexmap::{IndexMap, IndexSet};
    use itertools::Itertools;

    use crate::{
        keyed::distributed::messages::{
            DoneMessage, LocalOutgoingMessage, RemoteMessage, TargetedMessage,
        },
        time::NoTime,
        NoData,
    };

    use super::*;
    // a partitioner that just uses the key as a wrapping index
    fn partiton_index<'a>(i: &usize, s: &'a IndexSet<WorkerId>) -> &'a WorkerId {
        s.get_index(i % s.len()).unwrap()
    }

    fn make_upscale_distributor() -> CollectDistributor<usize, NoData, NoTime> {
        CollectDistributor::new(
            0,
            IndexSet::from([1]),
            IndexSet::from([0]),
            IndexSet::from([0, 1]),
            0,
            IndexMap::new(),
            Vec::new(),
        )
    }

    /// Check the stored version gets incremented by one
    #[test]
    fn increases_version_on_new() {
        let dist = make_upscale_distributor();
        assert_eq!(dist.version, 1)
    }

    // /// It must create a complete version map on creation so we can know,
    // /// when we are done
    // #[test]
    // fn creates_version_set_on_new() {
    //     let dist: CollectDistributor<usize, NoData, NoTime> = CollectDistributor::new(
    //         0,
    //         IndexSet::from([]),
    //         IndexSet::from([0, 1, 2, 3]),
    //         IndexSet::from([0]),
    //         0,
    //         IndexMap::from([(2, 1)]),
    //         Vec::new(),
    //     );
    //     assert_eq!(
    //         dist.remote_versions,
    //         IndexMap::from([(1, 0), (2, 1), (3, 0)])
    //     );
    // }

    /// Should respect Rule 1.1
    /// • Rule 1.1: If (F'(K) != Local) && K ∈ whitelist
    /// • We will not have the state under the new configuration, but currently it is still located
    /// here  -> pass downstream
    #[test]
    fn handle_data_rule_1_1() {
        let mut dist = make_upscale_distributor();
        dist.whitelist.insert(1);
        let msg = DataMessage {
                key: 1,
                value: NoData,
                timestamp: NoTime,
            };
        let msg = dist.handle_msg(msg, None, 0, partiton_index);
        assert!(matches!(msg, OutgoingMessage::Local(_)));
    }

    /// Should respect Rule 1.2
    /// Rule 1.2: (F'(K) != Local) && K ∈ hold
    /// • We will not have the state under the new configuration, but currently it is being collected
    /// here
    /// • -> buffer the message
    #[test]
    fn handle_data_rule_1_2() {
        let mut dist = make_upscale_distributor();
        dist.hold.insert(1, Vec::new());
        let msg = IncomingMessage::Data(
            crate::DataMessage {
                key: 1,
                value: NoData,
                timestamp: NoTime,
            },
            None,
        );
        let (dist, msg) = dist.handle_msg(msg, 0, partiton_index);
        let dist = match (dist, msg) {
            (DistributorKind::Collect(x), OutgoingMessage::None) => x,
            _ => panic!(),
        };
        assert_eq!(dist.hold.get(&1).unwrap().len(), 1);
    }

    ///Rule 2: (F'(K) != Local) && K ∉ whitelist && K ∉ hold
    /// • We do not have state for this key and we will not have
    ///   it under the new configuration • -> distribute via F'
    #[test]
    fn handle_data_rule_2() {
        let dist = make_upscale_distributor();
        let msg = IncomingMessage::Data(
            crate::DataMessage {
                key: 1,
                value: NoData,
                timestamp: NoTime,
            },
            None,
        );
        let (dist, msg) = dist.handle_msg(msg, 0, partiton_index);
        match (dist, msg) {
            (
                DistributorKind::Collect(_),
                OutgoingMessage::Remote(RemoteMessage::Data(TargetedMessage {
                    target: 1,
                    version: 1,
                    message: _,
                })),
            ) => (),
            _ => panic!(),
        };
    }

    /// Rule 3
    /// Rule 3: (F'(K) == Local)
    /// • if F(K) == Sender: -> pass downstream • else: distribute the message via F
    #[test]
    fn handle_data_rule_3() {
        let dist = make_upscale_distributor();
        let msg = IncomingMessage::Data(
            crate::DataMessage {
                key: 0,
                value: NoData,
                timestamp: NoTime,
            },
            None,
        );
        // should pass downstream since F'(0) == 0 and F(0) == 0 (sender)
        let (dist, msg) = dist.handle_msg(msg, 0, partiton_index);
        let dist = match (dist, msg) {
            (
                DistributorKind::Collect(x),
                OutgoingMessage::Local(LocalOutgoingMessage::Data(_)),
            ) => x,
            _ => panic!(),
        };

        let msg = IncomingMessage::Data(
            crate::DataMessage {
                key: 0,
                value: NoData,
                timestamp: NoTime,
            },
            None,
        );
        // should send to 1 since F'(0) == 0 but F(1) == 1
        let (dist, msg) = dist.handle_msg(msg, 0, partiton_index);
        match (dist, msg) {
            (
                DistributorKind::Collect(_),
                OutgoingMessage::Remote(RemoteMessage::Data(TargetedMessage {
                    target: 1,
                    version: _,
                    message: _,
                })),
            ) => (),
            _ => panic!(),
        };
    }

    /// Should create a Collector message and send it downstream locally
    #[test]
    fn sends_initial_collector() {
        let dist: CollectDistributor<usize, NoData, NoTime> = CollectDistributor::new(
            0,
            IndexSet::from([1, 3]),
            IndexSet::from([0]),
            IndexSet::from([0, 1]),
            0,
            IndexMap::new(),
            Vec::new(),
        );
        let (_, mut msg) = dist.run(partiton_index);

        assert_eq!(msg.len(), 1);
        let key = match msg.pop().unwrap() {
            OutgoingMessage::Local(LocalOutgoingMessage::Collect(c)) => c.key,
            _ => panic!(),
        };
        assert_eq!(key, 3)
    }

    /// should send another collector message once the initial one is done
    #[test]
    fn sends_next_collector() {
        let dist: CollectDistributor<usize, NoData, NoTime> = CollectDistributor::new(
            0,
            IndexSet::from([1, 3]),
            IndexSet::from([0]),
            IndexSet::from([0, 1]),
            0,
            IndexMap::new(),
            Vec::new(),
        );
        let (dist, mut msg) = dist.run(partiton_index);
        let key = match msg.pop().unwrap() {
            OutgoingMessage::Local(LocalOutgoingMessage::Collect(c)) => c.key,
            _ => panic!(),
        };
        assert_eq!(key, 3);
        // droping the collector should prompt it to send another one downstream
        drop(msg);
        let dist = match dist {
            DistributorKind::Collect(x) => x,
            _ => panic!(),
        };
        let (_, mut msg) = dist.run(partiton_index);
        let key = match msg.pop().unwrap() {
            OutgoingMessage::Local(LocalOutgoingMessage::Collect(c)) => c.key,
            _ => panic!(),
        };
        assert_eq!(key, 1)
    }

    /// Should create an Acquire, Drop_key message once the collector is done
    /// and also send out queued message
    #[test]
    fn creates_acquire_and_dropkey_message() {
        let dist: CollectDistributor<usize, NoData, NoTime> = CollectDistributor::new(
            0,
            IndexSet::from([1, 3]),
            IndexSet::from([0]),
            IndexSet::from([0, 1]),
            0,
            IndexMap::new(),
            Vec::new(),
        );
        let (dist, mut msg) = dist.run(partiton_index);

        assert_eq!(msg.len(), 1);
        let (dist, collector) = match (dist, msg.pop().unwrap()) {
            (
                DistributorKind::Collect(x),
                OutgoingMessage::Local(LocalOutgoingMessage::Collect(c)),
            ) => (x, c),
            _ => panic!(),
        };
        assert_eq!(collector.key, 3);
        // we now hold a collector with key, one, so all messages with that key should be held up
        let (dist, msg) = dist.handle_msg(
            IncomingMessage::Data(
                DataMessage {
                    key: 1,
                    value: NoData,
                    timestamp: NoTime,
                },
                None,
            ),
            0,
            partiton_index,
        );
        assert!(matches!(msg, OutgoingMessage::None));

        let dist = match dist {
            DistributorKind::Collect(x) => x,
            _ => panic!(),
        };

        // now we drop the collector
        drop(collector);
        // so running should now give us
        // - An Acquire(key: 1)
        // - All held messages for key: 1
        // - A DropKey(key: 1)

        let (_, mut messages) = dist.run(partiton_index);
        assert_eq!(messages.len(), 3);
        let acquire = messages.remove(0);
        let held = messages.remove(1);
        let dropkey = messages.remove(2);

        match acquire {
            OutgoingMessage::Remote(RemoteMessage::Acquire(a)) => assert_eq!(a.key, 1),
            _ => panic!(),
        }

        assert!(matches!(
            held,
            OutgoingMessage::Remote(RemoteMessage::Data(_))
        ));
        assert!(matches!(
            dropkey,
            OutgoingMessage::Local(LocalOutgoingMessage::DropKey(1))
        ));
    }

    /// Should send a done message once all state is collected
    #[test]
    fn sends_done_message() {
        let dist: CollectDistributor<usize, NoData, NoTime> = CollectDistributor::new(
            0,
            IndexSet::from([1]),
            IndexSet::from([0]),
            IndexSet::from([0, 1]),
            0,
            IndexMap::new(),
            Vec::new(),
        );
        let (dist, mut msg) = dist.run(partiton_index);

        assert_eq!(msg.len(), 1);
        let (dist, collector) = match (dist, msg.pop().unwrap()) {
            (
                DistributorKind::Collect(x),
                OutgoingMessage::Local(LocalOutgoingMessage::Collect(c)),
            ) => (x, c),
            _ => panic!(),
        };
        // now we drop the collector
        drop(collector);
        // so running should now give us
        // - An Acquire(key: 1)
        // - All held messages for key: 1
        // - A DropKey(key: 1)
        // - A DoneMessage
        let (_, mut messages) = dist.run(partiton_index);
        assert_eq!(messages.len(), 4);
        let done = messages.remove(3);

        match done {
            OutgoingMessage::Remote(RemoteMessage::Done(DoneMessage {
                worker_id: 0,
                version: 1,
            })) => (),
            _ => panic!(),
        }
    }

    /// Should transform back into a normal distributor once everythig is done
    #[test]
    fn turns_into_normal_dist() {
        let dist: CollectDistributor<usize, NoData, NoTime> = CollectDistributor::new(
            0,
            IndexSet::from([1]),
            IndexSet::from([0]),
            IndexSet::from([0, 1]),
            0,
            IndexMap::new(),
            Vec::new(),
        );
        let (dist, mut msg) = dist.run(partiton_index);

        assert_eq!(msg.len(), 1);
        let (dist, collector) = match (dist, msg.remove(0)) {
            (
                DistributorKind::Collect(x),
                OutgoingMessage::Local(LocalOutgoingMessage::Collect(c)),
            ) => (x, c),
            _ => panic!(),
        };
        drop(collector);

        // so running should now give us
        // - An Acquire(key: 1)
        // - All held messages for key: 1
        // - A DropKey(key: 1)
        // - A DoneMessage
        let (dist, _) = dist.run(partiton_index);
        let dist = match dist {
            DistributorKind::Collect(x) => x,
            _ => panic!(),
        };
        let (dist, msg) = dist.handle_msg(
            IncomingMessage::Done(DoneMessage {
                worker_id: 1,
                version: 1,
            }),
            1,
            partiton_index,
        );
        match (dist, msg) {
            (DistributorKind::Normal(_), OutgoingMessage::None) => (),
            _ => panic!(),
        };
    }
}
