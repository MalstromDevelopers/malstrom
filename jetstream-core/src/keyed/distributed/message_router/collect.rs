use indexmap::IndexSet;

use crate::{
    channels::selective_broadcast::Output,
    keyed::distributed::{NetworkDataMessage, Remotes},
    runtime::communication::broadcast,
    types::*,
};

use super::super::types::*;
use super::{finished::FinishedRouter, MessageRouter, NetworkMessage};

#[derive(Debug)]
pub(crate) struct CollectRouter<K, V, T> {
    pub(super) version: Version,

    pub(super) whitelist: IndexSet<K>, // pub for testing
    old_worker_set: IndexSet<WorkerId>,
    new_worker_set: IndexSet<WorkerId>,
    /// Datamessages for the key we are currently collecting
    buffered: Vec<DataMessage<K, V, T>>,

    current_collect: Option<Collect<K>>,

    // these are control messages we can not handle while rescaling
    // so we will buffer them, waiting for the normal dist to deal with them
    pub(super) queued_rescales: Vec<RescaleMessage>,
}

impl<K, V, T> CollectRouter<K, V, T>
where
    K: Key,
{
    pub(super) fn new(
        version: Version,
        whitelist: IndexSet<K>,
        old_worker_set: IndexSet<WorkerId>,
        new_worker_set: IndexSet<WorkerId>,
        queued_rescales: Vec<RescaleMessage>,
    ) -> Self {
        Self {
            version: version + 1,
            whitelist,
            old_worker_set,
            new_worker_set,
            buffered: Vec::new(),
            current_collect: None,
            queued_rescales,
        }
    }

    pub(super) fn route_message(
        &mut self,
        msg: DataMessage<K, V, T>,
        partitioner: WorkerPartitioner<K>,
        this_worker: WorkerId,
        sender: WorkerId,
    ) -> Option<(DataMessage<K, V, T>, WorkerId)> {
        let key = &msg.key;
        let new_target = partitioner(key, &self.new_worker_set);

        let in_whitelist = self.whitelist.contains(key);
        let to_be_buffered = self
            .current_collect
            .as_ref()
            .map_or(false, |collect| &collect.key == key);

        match (new_target == this_worker, in_whitelist, to_be_buffered) {
            // Rule 1.1, non-local && in_whitelist
            (false, true, _) => Some((msg, this_worker)),
            // Rule 1.2
            (false, false, true) => {
                self.buffered.push(msg);
                None
            }
            // Rule 2
            (false, false, false) => Some((msg, new_target)),
            // Rule 3
            (true, _, _) => {
                let old_target = partitioner(key, &self.old_worker_set);
                // TODO: memoize these keys
                if old_target == sender {
                    Some((msg, this_worker))
                } else {
                    Some((msg, old_target))
                }
            }
        }
    }
}

impl<K, V, T> CollectRouter<K, V, T>
where
    K: DistKey,
    V: DistData,
    T: DistTimestamp,
{
    pub(crate) fn lifecycle(
        mut self,
        partitioner: WorkerPartitioner<K>,
        output: &mut Output<K, V, T>,
        remotes: &Remotes<K, V, T>,
    ) -> MessageRouter<K, V, T> {
        // try finishing the state collection
        match self.current_collect.take().map(NetworkAcquire::try_from) {
            Some(Ok(acquire)) => {
                let target = partitioner(&acquire.key, &self.new_worker_set);
                let target_client = &remotes
                    .get(&target)
                    .expect("partitioner returns valid target")
                    .0;
                target_client.send(NetworkMessage::Acquire(acquire));
                for buffered_msg in self.buffered.drain(..) {
                    let net_msg = NetworkDataMessage::new(buffered_msg, self.version);
                    target_client.send(NetworkMessage::Data(net_msg));
                }
                self.set_and_emit_collect(output);
            }
            Some(Err(collect)) => {
                self.current_collect = Some(collect);
            }
            None => self.set_and_emit_collect(output),
        }

        if self.current_collect.is_none() && self.whitelist.is_empty() {
            broadcast(
                remotes.values().map(|(client, _)| client),
                NetworkMessage::Upgrade(self.version),
            );
            MessageRouter::Finished(FinishedRouter::new(
                self.version,
                self.old_worker_set,
                self.new_worker_set,
                self.queued_rescales,
            ))
        } else {
            MessageRouter::Collect(self)
        }
    }

    fn set_and_emit_collect(&mut self, output: &mut Output<K, V, T>) {
        if self.current_collect.is_none() {
            self.current_collect = self.whitelist.pop().map(Collect::new).map(|collect| {
                output.send(Message::Collect(collect.clone()));
                collect
            });
        }
    }
}

#[cfg(test)]
mod test {

    use crate::{
        channels::selective_broadcast::{full_broadcast, link, Input},
        runtime::CommunicationClient,
        testing::{FakeCommunication, SentMessage},
    };

    use super::*;

    // a partitioner that just uses the key as a wrapping index
    fn partiton_index<'a>(i: &usize, s: &'a IndexSet<WorkerId>) -> WorkerId {
        *s.get_index(i % s.len()).unwrap()
    }

    fn get_input_output<K: MaybeKey, V: MaybeData, T: MaybeTime>(
    ) -> (Output<K, V, T>, Input<K, V, T>) {
        let mut sender = Output::new_unlinked(full_broadcast);
        let mut receiver = Input::new_unlinked();
        link(&mut sender, &mut receiver);
        (sender, receiver)
    }

    #[test]
    fn increases_version_on_new() {
        let dist: CollectRouter<i32, NoData, usize> = CollectRouter::new(
            0,
            IndexSet::new(),
            IndexSet::new(),
            IndexSet::new(),
            Vec::new(),
        );
        assert_eq!(dist.version, 1)
    }

    /// Should respect Rule 1.1
    /// • Rule 1.1: If (F'(K) != Local) && K ∈ whitelist
    /// • We will not have the state under the new configuration, but currently it is still located
    /// here  -> pass downstream
    #[test]
    fn handle_data_rule_1_1() {
        let key = 15;
        let mut dist: CollectRouter<usize, i32, usize> = CollectRouter::new(
            0,
            IndexSet::from([key.clone()]),
            IndexSet::from([0]),
            IndexSet::from([0, 1]),
            Vec::new(),
        );

        let msg = DataMessage {
            key,
            value: 222,
            timestamp: 512,
        };
        let result = dist.route_message(msg.clone(), partiton_index, 0, 0);
        match result {
            Some((out_msg, target)) => {
                assert_eq!(target, 0);
                assert_eq!(out_msg, msg);
            }
            None => panic!(),
        }
    }

    /// Should respect Rule 1.2
    /// Rule 1.2: (F'(K) != Local) && K ∈ hold
    /// • We will not have the state under the new configuration, but currently it is being collected
    /// here
    /// • -> buffer the message
    #[test]
    fn handle_data_rule_1_2() {
        let key = 15;
        let dist: CollectRouter<usize, i32, usize> = CollectRouter::new(
            0,
            IndexSet::from([key.clone()]),
            IndexSet::from([0]),
            IndexSet::from([0, 1]),
            Vec::new(),
        );

        let mut comm = FakeCommunication::<NetworkMessage<usize, i32, usize>>::default();
        let mut remotes: Remotes<usize, i32, usize> = Remotes::new();
        remotes.insert(
            1,
            (
                CommunicationClient::new(1, 0, &mut comm).unwrap(),
                RemoteState::default(),
            ),
        );

        let (mut sender, mut receiver) = get_input_output();

        let mut dist = dist.lifecycle(partiton_index, &mut sender, &remotes);

        let collector = receiver.recv().unwrap();
        assert!(matches!(collector, Message::Collect(_)));

        let msg = DataMessage::new(key, 22, 555);
        let out = dist.route_message(msg, None, partiton_index, 0, 0);
        assert!(out.is_none());

        // drop the collector, next lifecycle should emit the acquire
        drop(collector);
        let _ = dist.lifecycle(partiton_index, &mut sender, &remotes);

        let acquire = comm.recv_from_operator().unwrap();
        assert_eq!(acquire.to_operator, 0);
        assert_eq!(acquire.to_worker, 1);
        match acquire.msg {
            NetworkMessage::Acquire(a) => {
                assert_eq!(a.key, key)
            }
            _ => panic!(),
        }
    }

    /// Rule 2: (F'(K) != Local) && K ∉ whitelist && K ∉ hold
    /// • We do not have state for this key and we will not have
    ///   it under the new configuration • -> distribute via F'
    #[test]
    fn handle_data_rule_2() {
        let mut dist: CollectRouter<usize, i32, usize> = CollectRouter::new(
            0,
            IndexSet::from([3]),
            IndexSet::from([0]),
            IndexSet::from([0, 1]),
            Vec::new(),
        );
        let msg = DataMessage {
            key: 7,
            value: 42,
            timestamp: 512,
        };

        let (out_msg, target) = dist
            .route_message(msg.clone(), partiton_index, 0, 0)
            .unwrap();
        assert_eq!(out_msg, msg);
        assert_eq!(target, 1);
    }

    /// Rule 3
    /// Rule 3: (F'(K) == Local)
    /// • if F(K) == Sender: -> pass downstream • else: distribute the message via F
    #[test]
    fn handle_data_rule_3() {
        let mut dist: CollectRouter<usize, i32, usize> = CollectRouter::new(
            0,
            IndexSet::from([3]),
            IndexSet::from([0, 1]),
            IndexSet::from([0]),
            Vec::new(),
        );

        // this should go downstream
        let msg = DataMessage {
            key: 2,
            value: 42,
            timestamp: 512,
        };
        let (out_msg, target) = dist
            .route_message(msg.clone(), partiton_index, 0, 0)
            .unwrap();
        assert_eq!(out_msg, msg);
        assert_eq!(target, 0);

        // this should go to 1
        let msg = DataMessage {
            key: 3,
            value: 42,
            timestamp: 512,
        };
        let (out_msg, target) = dist
            .route_message(msg.clone(), partiton_index, 0, 0)
            .unwrap();
        assert_eq!(out_msg, msg);
        assert_eq!(target, 1);
    }

    /// Should create collectors and and send them downstream
    #[test]
    fn creates_collectors() {
        let dist: CollectRouter<usize, i32, usize> = CollectRouter::new(
            0,
            IndexSet::from([1, 3, 5]),
            IndexSet::from([0]),
            IndexSet::from([0, 1]),
            Vec::new(),
        );

        let mut comm = FakeCommunication::<NetworkMessage<usize, i32, usize>>::default();
        let mut remotes: Remotes<usize, i32, usize> = Remotes::new();
        remotes.insert(
            1,
            (
                CommunicationClient::new(1, 0, &mut comm).unwrap(),
                RemoteState::default(),
            ),
        );

        let (mut sender, mut receiver) = get_input_output();
        let dist = dist.lifecycle(partiton_index, &mut sender, &remotes);
        assert!(matches!(
            receiver.recv().unwrap(),
            Message::Collect(Collect { key: 5, .. })
        ));
        let dist = dist.lifecycle(partiton_index, &mut sender, &remotes);
        assert!(matches!(
            receiver.recv().unwrap(),
            Message::Collect(Collect { key: 3, .. })
        ));
        dist.lifecycle(partiton_index, &mut sender, &remotes);
        assert!(matches!(
            receiver.recv().unwrap(),
            Message::Collect(Collect { key: 1, .. })
        ));
    }

    /// Should create an acquire message and emit buffered messages
    #[test]
    fn creates_acquire_and_emits_buffers() {
        let dist: CollectRouter<usize, i32, usize> = CollectRouter::new(
            0,
            IndexSet::from([1]),
            IndexSet::from([0]),
            IndexSet::from([0, 1]),
            Vec::new(),
        );

        let mut comm = FakeCommunication::<NetworkMessage<usize, i32, usize>>::default();
        let mut remotes: Remotes<usize, i32, usize> = Remotes::new();
        remotes.insert(
            1,
            (
                CommunicationClient::new(1, 0, &mut comm).unwrap(),
                RemoteState::default(),
            ),
        );

        let (mut sender, mut receiver) = get_input_output();

        let mut dist = dist.lifecycle(partiton_index, &mut sender, &remotes);

        let mut collector = match receiver.recv().unwrap() {
            Message::Collect(c) => c,
            _ => panic!(),
        };

        // this message should get buffered
        let buffered_msg = DataMessage::new(1, 22, 33);
        dist.route_message(buffered_msg.clone(), None, partiton_index, 0, 0);

        collector.add_state(25, "foobar".to_string());
        // drop it to trigger the acquire
        drop(collector);
        dist.lifecycle(partiton_index, &mut sender, &remotes);

        let acquire = comm.recv_from_operator().unwrap();
        assert_eq!(acquire.to_worker, 1);
        assert_eq!(acquire.to_operator, 0);

        match acquire.msg {
            NetworkMessage::Acquire(a) => {
                let local_acquire = Acquire::from(a);
                let (key, state): (usize, String) = local_acquire.take_state(&25).unwrap();
                assert_eq!(key, 1);
                assert_eq!(state, "foobar");
            }
            _ => panic!(),
        }

        // buffered data
        let data = comm.recv_from_operator().unwrap().msg;
        match data {
            NetworkMessage::Data(d) => {
                assert_eq!(d.content, buffered_msg);
                assert_eq!(d.version, 1);
            }
            _ => panic!(),
        }
    }

    /// Should broadcast an "upgrade" message when done
    #[test]
    fn broadcast_update() {
        let dist: CollectRouter<usize, i32, usize> = CollectRouter::new(
            0,
            IndexSet::new(),
            IndexSet::from([0]),
            IndexSet::from([0, 1, 2]),
            Vec::new(),
        );

        let mut comm = FakeCommunication::<NetworkMessage<usize, i32, usize>>::default();
        let mut remotes: Remotes<usize, i32, usize> = Remotes::new();
        remotes.insert(
            1,
            (
                CommunicationClient::new(1, 0, &mut comm).unwrap(),
                RemoteState::default(),
            ),
        );
        remotes.insert(
            2,
            (
                CommunicationClient::new(2, 0, &mut comm).unwrap(),
                RemoteState::default(),
            ),
        );
        let (mut sender, _receiver) = get_input_output();
        // since there are no keys in whitelist this should be done immediatly
        dist.lifecycle(partiton_index, &mut sender, &remotes);

        let upgrade1 = comm.recv_from_operator().unwrap();
        let upgrade2 = comm.recv_from_operator().unwrap();

        let mut upgrade_messages = [upgrade1, upgrade2];
        upgrade_messages.sort_by(|a, b| a.to_worker.cmp(&b.to_worker));

        match upgrade_messages[0] {
            SentMessage {
                to_worker: wid,
                to_operator: oid,
                msg: NetworkMessage::Upgrade(v),
            } => {
                assert_eq!(wid, 1);
                assert_eq!(oid, 0);
                assert_eq!(v, 1);
            }
            _ => panic!(),
        }
        match upgrade_messages[1] {
            SentMessage {
                to_worker: wid,
                to_operator: oid,
                msg: NetworkMessage::Upgrade(v),
            } => {
                assert_eq!(wid, 2);
                assert_eq!(oid, 0);
                assert_eq!(v, 1);
            }
            _ => panic!(),
        }
    }

    /// check we transition back to a normal dist if all routers are done
    #[test]
    fn transitions_to_normal() {
        let dist: CollectRouter<usize, i32, usize> = CollectRouter::new(
            0,
            IndexSet::new(),
            IndexSet::from([0]),
            IndexSet::from([0, 1]),
            Vec::new(),
        );

        let mut comm = FakeCommunication::<NetworkMessage<usize, i32, usize>>::default();
        let mut remotes: Remotes<usize, i32, usize> = Remotes::new();
        remotes.insert(
            1,
            (
                CommunicationClient::new(1, 0, &mut comm).unwrap(),
                RemoteState::default(),
            ),
        );
        let (mut sender, _receiver) = get_input_output();

        // tell it worker 1 is done with rescaling
        remotes.get_mut(&1).unwrap().1.last_version = 1;

        let dist = dist.lifecycle(partiton_index, &mut sender, &remotes);
        assert!(matches!(dist, MessageRouter::Finished(_)));
        let dist = dist.lifecycle(partiton_index, &mut sender, &remotes);
        assert!(matches!(dist, MessageRouter::Normal(_)), "{dist:?}");
    }
}
