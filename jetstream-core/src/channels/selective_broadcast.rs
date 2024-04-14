use std::{
    collections::VecDeque,
    ops::Deref,
    rc::Rc,
    sync::atomic::{AtomicUsize, Ordering},
};

/// Selective Broadcast:
///
/// Selective Broadcast channels are MPMC channels, where a partitioning function is used
/// to determine which receivers shall receive a value.
/// The value is copied as often as necessary, to ensure multiple receivers can receive it.
/// NOTE: This has absolutely NO guards against slow consumers. Slow consumers can build very
/// big queues with this channel.
use crossbeam;
use indexmap::{IndexMap, IndexSet};
use itertools::{self};
use serde_json::value::Index;
use url::form_urlencoded::Target;

use crate::{
    snapshot::Barrier,
    time::{MaybeTime, NoTime, Timestamp},
    Message, OperatorId, OperatorPartitioner, Scale, ShutdownMarker,
};

static COUNTER: AtomicUsize = AtomicUsize::new(0);
/// This is a somewhat hacky we to get a unique id for each sender, which we
/// use to identify messages downstream
/// Taken from https://users.rust-lang.org/t/idiomatic-rust-way-to-generate-unique-id/33805
fn get_id() -> usize {
    COUNTER.fetch_add(1, Ordering::Relaxed)
}

enum MessageWrapper<K, V, T> {
    /// Normal JetStream message with sender id
    Message(usize, Message<K, V, T>),
    /// Sender sends this to the receiver once it gets unlinked
    Register(usize),
    Deregister(usize),
}

// /// A simple partitioner, which will broadcast a value to all receivers
pub fn full_broadcast<T>(_: &T, scale: Scale) -> Vec<OperatorId> {
    (0..scale).collect()
}

/// Link a Sender and receiver together
pub fn link<K, V, T>(sender: &mut Sender<K, V, T>, receiver: &mut Receiver<K, V, T>) {
    let tx = receiver.get_sender();
    let _ = tx.send(MessageWrapper::Register(sender.id));
    sender.senders.push(tx)
}

/// Selective Broadcast Sender
pub struct Sender<K, V, T> {
    // TOOD: We only have the partitioner in the Box to allow cloning
    // Which is only really needed in the snapshot conroller
    // Check if we can solve that another way
    senders: Vec<crossbeam::channel::Sender<MessageWrapper<K, V, T>>>,
    #[allow(clippy::type_complexity)] // it's not thaaat complex
    partitioner: Rc<dyn OperatorPartitioner<K, V, T>>,
    /// uniqe id of this sender
    id: usize,
}

impl<K, V, T> Sender<K, V, T>
where
    K: Clone,
    V: Clone,
    T: Clone,
{
    pub fn new_unlinked(partitioner: impl OperatorPartitioner<K, V, T>) -> Self {
        Self {
            senders: Vec::new(),
            partitioner: Rc::new(partitioner),
            id: get_id(),
        }
    }

    /// Send a value into this channel.
    /// Data messages are distributed as per the partioning function
    /// System messages are always broadcasted
    pub fn send(&mut self, msg: Message<K, V, T>) {
        if self.senders.is_empty() {
            return;
        }
        let recipient_len = self.senders.len();
        match msg {
            Message::Data(x) => {
                let indices = (self.partitioner)(&x, recipient_len);
                let l = indices.len();
                for (i, msg) in indices
                    .into_iter()
                    .zip(itertools::repeat_n(Message::Data(x), l))
                {
                    let s = self.senders.get(i).expect("Partitioner index out of range");
                    let _ = s.send(MessageWrapper::Message(self.id, msg));
                }

                // for (sender, elem) in self
                //     .senders
                //     .iter_mut()
                //     .zip(itertools::repeat_n(Message::Data(x), indices.len()))
                // {
                //     let _ = sender.send(elem);
                // }
            }
            _ => {
                // repeat_n will clone for every iteration except the last
                // this gives us a small optimization on the common "1 receiver" case :)
                for (sender, elem) in self
                    .senders
                    .iter_mut()
                    .zip(itertools::repeat_n(msg, recipient_len))
                {
                    let _ = sender.send(MessageWrapper::Message(self.id, elem));
                }
            }
        };
    }
}

impl<K, V, T> Clone for Sender<K, V, T> {
    fn clone(&self) -> Self {
        let id = get_id();
        for s in &self.senders {
            let _ = s.send(MessageWrapper::Register(id));
        }
        Self {
            senders: self.senders.clone(),
            partitioner: self.partitioner.clone(),
            id,
        }
    }

    // fn subscribe_inner(&mut self) -> crossbeam::channel::Receiver<Message<K, V, T>> {
    //     let (tx, rx) = crossbeam::channel::unbounded();
    //     tx.send(MessageWrapper::Register(self.id));
    //     self.senders.push(tx);
    //     rx
    // }
}

impl<K, V, T> Drop for Sender<K, V, T> {
    fn drop(&mut self) {
        for s in self.senders.iter() {
            let _ = s.send(MessageWrapper::Deregister(self.id));
        }
    }
}

struct UpstreamState<T> {
    barrier: Option<Barrier>,
    epoch: Option<T>,
    shutdown_marker: Option<ShutdownMarker>,
}
impl<T> UpstreamState<T> {
    fn needs_alignement(&self) -> bool {
        self.barrier.is_some() || self.shutdown_marker.is_some()
    }
}
impl<T> Default for UpstreamState<T> {
    fn default() -> Self {
        Self {
            barrier: None,
            epoch: None,
            shutdown_marker: None,
        }
    }
}
/// Selective Broadcast Receiver
pub struct Receiver<K, V, T> {
    /// this sender can be cloned to send messages to this receiver
    sender: crossbeam::channel::Sender<MessageWrapper<K, V, T>>,
    receiver: crossbeam::channel::Receiver<MessageWrapper<K, V, T>>,
    states: IndexMap<usize, UpstreamState<T>>,
    // messages we are buffering, because we need alignement
    // from upstream to issue them
    buffered: VecDeque<Message<K, V, T>>,
}
// impl<K, V, T> Receiver<K, V, T> where T: MaybeTime  {
//     /// update the internally stored epoch and emit a copy
//     /// if it increased or None if it did not increase
//     fn has(&self) -> Option<T> {
//         self.inner_receivers.iter().map(|x|)
//         match self.merged_epoch.take() {
//             Some(e) => {
//                 let merged = e.try_merge(&new)?;
//                 if merged > e {
//                     self.merged_epoch = Some(merged.clone());
//                     Some(merged)
//                 } else {
//                     self.merged_epoch = Some(e);
//                     None
//                 }
//             },
//             None => {
//                 self.merged_epoch = Some(new.clone());
//                 Some(new)
//             },
//         }
//     }
// }
impl<K, V, T> Receiver<K, V, T> {
    pub fn new_unlinked() -> Self {
        let (sender, receiver) = crossbeam::channel::unbounded();
        Self {
            sender,
            receiver,
            states: IndexMap::new(),
            buffered: VecDeque::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.receiver.is_empty()
    }

    fn get_sender(&self) -> crossbeam::channel::Sender<MessageWrapper<K, V, T>> {
        self.sender.clone()
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

fn handle_received<K, V, T: MaybeTime>(
    states: &mut IndexMap<usize, UpstreamState<T>>,
    buffer: &mut VecDeque<Message<K, V, T>>,
    sender: usize,
    msg: Message<K, V, T>,
) -> Option<Message<K, V, T>> {
    // PANIC: Caller guarantees valid index
    let state = states.get_mut(&sender).unwrap();
    if state.needs_alignement() {
        buffer.push_back(msg);
        return None;
    }
    match msg {
        Message::Epoch(e) => {
            state.epoch = Some(e);
            let merged = merge_timestamps(states.values().map(|x| &x.epoch));
            if let Some(x) = merged {
                Some(Message::Epoch(x.clone()))
            } else {
                None
            }
        }
        Message::AbsBarrier(b) => {
            state.barrier = Some(b);
            if states.values().all(|x| x.barrier.is_some()) {
                states
                    .values_mut()
                    .map(|x| x.barrier.take())
                    .last()
                    .flatten()
                    .map(|x| Message::AbsBarrier(x))
            } else {
                None
            }
        }
        Message::ShutdownMarker(s) => {
            state.shutdown_marker = Some(s);
            if states.values().all(|x| x.shutdown_marker.is_some()) {
                states
                    .values_mut()
                    .map(|x| x.shutdown_marker.take())
                    .last()
                    .flatten()
                    .map(|x| Message::ShutdownMarker(x))
            } else {
                None
            }
        }
        x => Some(x),
    }
}

impl<K, V, T> Receiver<K, V, T>
where
    T: MaybeTime,
{
    /// Receive a value. None if no value to receive or all Senders dropped.
    ///
    /// This method synchronizes barriers, i.e. if a channel is barred, it will
    /// not receive any messages from that channel until all channels are barred.
    /// Once all channels are barred, a single barrier will be emitted
    pub fn recv(&mut self) -> Option<Message<K, V, T>> {
        // there are messages in the buffer, but no state needs alignemnet
        // i.e. any alignement was resolved on the previous call
        if self.buffered.len() > 0 && self.states.values().all(|x| !x.needs_alignement()) {
            return self.buffered.pop_front();
        }

        while let Ok(msg_wrapper) = self.receiver.try_recv() {
            let out = match msg_wrapper {
                MessageWrapper::Message(i, msg) => {
                    handle_received(&mut self.states, &mut self.buffered, i, msg)
                }
                MessageWrapper::Register(i) => {
                    self.states.insert(i, UpstreamState::default());
                    None
                }
                MessageWrapper::Deregister(i) => {
                    self.states.swap_remove(&i);
                    None
                }
            };
            if out.is_some() {
                return out;
            }
        }
        None
    }
}

#[cfg(test)]
mod test {
    use crate::{snapshot::NoPersistence, time::NoTime, DataMessage, NoData, NoKey};

    use super::*;

    /// Check we can clone a sender
    #[test]
    fn clone_sender() {
        let mut sender = Sender::new_unlinked(full_broadcast);
        let mut receiver = Receiver::new_unlinked();
        link(&mut sender, &mut receiver);

        let mut cloned = sender.clone();
        let msg = Message::Data(crate::DataMessage {
            key: NoKey,
            value: "Hello",
            timestamp: NoTime,
        });
        cloned.send(msg.clone());

        assert!(matches!(
            receiver.recv(),
            Some(Message::Data(crate::DataMessage {
                key: NoKey,
                value: "Hello",
                timestamp: NoTime
            }))
        ));

        sender.send(msg);
        assert!(matches!(
            receiver.recv(),
            Some(Message::Data(crate::DataMessage {
                key: NoKey,
                value: "Hello",
                timestamp: NoTime
            }))
        ));
        assert_eq!(receiver.states.len(), 2);
    }

    /// Check we only emit an epoch when it changes
    #[test]
    fn emit_epoch_on_change() {
        let mut sender: Sender<NoKey, NoData, i32> = Sender::new_unlinked(full_broadcast);
        let mut receiver = Receiver::new_unlinked();
        link(&mut sender, &mut receiver);
        let mut sender2 = sender.clone();

        sender.send(Message::Epoch(42));

        assert!(receiver.recv().is_none());
        sender2.send(Message::Epoch(15));
        assert!(matches!(receiver.recv(), Some(Message::Epoch(15))));
    }

    /// only issue a barried once it is aligned
    #[test]
    fn aligns_barriers() {
        let mut sender: Sender<NoKey, NoData, i32> = Sender::new_unlinked(full_broadcast);
        let mut receiver = Receiver::new_unlinked();
        link(&mut sender, &mut receiver);
        let mut sender2 = sender.clone();

        sender.send(Message::AbsBarrier(Barrier::new(Box::new(
            NoPersistence::default(),
        ))));

        let received = receiver.recv();
        assert!(received.is_none(), "{received:?}");
        sender2.send(Message::AbsBarrier(Barrier::new(Box::new(
            NoPersistence::default(),
        ))));

        assert!(matches!(receiver.recv(), Some(Message::AbsBarrier(_))));
    }

    /// should buffer messages if the channels if barred
    #[test]
    fn buffer_on_barriers() {
        let mut sender: Sender<NoKey, i32, NoTime> = Sender::new_unlinked(full_broadcast);
        let mut receiver = Receiver::new_unlinked();
        link(&mut sender, &mut receiver);
        let mut sender2 = sender.clone();

        sender.send(Message::AbsBarrier(Barrier::new(Box::new(
            NoPersistence::default(),
        ))));

        sender.send(Message::Data(DataMessage::new(NoKey, 42, NoTime)));
        sender.send(Message::Data(DataMessage::new(NoKey, 177, NoTime)));

        sender2.send(Message::AbsBarrier(Barrier::new(Box::new(
            NoPersistence::default(),
        ))));
        assert!(matches!(receiver.recv(), Some(Message::AbsBarrier(_))));
        assert!(matches!(
            receiver.recv(),
            Some(Message::Data(DataMessage {
                key: _,
                value: 42,
                timestamp: _
            }))
        ));
        assert!(matches!(
            receiver.recv(),
            Some(Message::Data(DataMessage {
                key: _,
                value: 177,
                timestamp: _
            }))
        ));
    }

    /// only issue shutdown markers once they are aligned
    #[test]
    fn aligns_shutdowns() {
        let mut sender: Sender<NoKey, NoData, i32> = Sender::new_unlinked(full_broadcast);
        let mut receiver = Receiver::new_unlinked();
        link(&mut sender, &mut receiver);
        let mut sender2 = sender.clone();

        sender.send(Message::ShutdownMarker(ShutdownMarker::default()));

        let received = receiver.recv();
        assert!(received.is_none(), "{received:?}");
        sender2.send(Message::ShutdownMarker(ShutdownMarker::default()));

        assert!(matches!(receiver.recv(), Some(Message::ShutdownMarker(_))));
    }

    /// Dropping a sender should remove its state from the receiver
    #[test]
    fn drop_sender() {
        let mut sender: Sender<NoKey, i32, NoTime> = Sender::new_unlinked(full_broadcast);
        let mut receiver = Receiver::new_unlinked();
        link(&mut sender, &mut receiver);
        sender.send(Message::Data(DataMessage::new(NoKey, 42, NoTime)));
        sender.send(Message::Data(DataMessage::new(NoKey, 177, NoTime)));
        drop(sender);

        assert!(matches!(
            receiver.recv(),
            Some(Message::Data(DataMessage {
                key: _,
                value: 42,
                timestamp: _
            }))
        ));
        assert!(matches!(
            receiver.recv(),
            Some(Message::Data(DataMessage {
                key: _,
                value: 177,
                timestamp: _
            }))
        ));
        receiver.recv();
        assert!(matches!(receiver.states.len(), 0));
    }
}
