//! Selective Broadcast:
//!
//! Selective Broadcast channels are MPMC channels, where a partitioning function is used
//! to determine which receivers shall receive a value.
//! The value is copied as often as necessary, to ensure multiple receivers can receive it.
//! NOTE: This has absolutely NO guards against slow consumers. Slow consumers can build very
//! big queues with this channel.
use std::{
    rc::Rc,
    sync::atomic::{AtomicUsize, Ordering},
};

use itertools::Itertools;

use super::spsc;

use crate::types::{MaybeTime, Message, OperatorId, OperatorPartitioner};

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
#[inline(always)]
pub fn full_broadcast<T>(_: &T, outputs: &mut [bool]) -> () {
    outputs.fill(true);
}

/// Link a Sender and receiver together
pub fn link<K, V, T>(sender: &mut Output<K, V, T>, receiver: &mut Input<K, V, T>) {
    let (tx, rx) = spsc::unbounded();
    sender.senders.push(tx);
    receiver.receivers.push(UpstreamState::new(rx));
}

// TODO DAMION: Fundamentally you need SPSC channels, which each
// Sender containing multiple channel senders and each Receiver
// containing multiple channel receivers, thus giving the illusion
// of mpmc while still allowing the receivers to select where they
// read from

/// Operator Output
pub struct Output<K, V, T> {
    // Each sender in this Vec is essentially one outgoing
    // edge from the operator
    senders: Vec<spsc::Sender<Message<K, V, T>>>,
    #[allow(clippy::type_complexity)] // it's not thaaat complex
    partitioner: Rc<dyn OperatorPartitioner<K, V, T>>,
    frontier: Option<T>,
}

impl<K, V, T> Output<K, V, T>
where
    K: Clone,
    V: Clone,
    T: Clone + MaybeTime,
{
    /// Create a new Sender with **no** associated Receiver
    /// Link a receiver with [link].
    pub fn new_unlinked(partitioner: impl OperatorPartitioner<K, V, T>) -> Self {
        Self {
            senders: Vec::new(),
            partitioner: Rc::new(partitioner),
            frontier: None,
        }
    }

    /// Send a value into this channel.
    /// Data messages are distributed as per the partioning function.
    ///
    /// System messages are always broadcasted.
    pub fn send(&mut self, msg: Message<K, V, T>) {
        if let Message::Epoch(e) = &msg {
            if self.frontier.as_ref().is_some_and(|x| e > x) || self.frontier.is_none() {
                self.frontier = Some(e.clone());
            }
        }

        if self.senders.is_empty() {
            return;
        }
        let recipient_len = self.senders.len();
        let mut output_flags = vec![false; recipient_len];
        match msg {
            Message::Data(x) => {
                (self.partitioner)(&x, &mut output_flags);
                let msg_count = output_flags.iter().map(|x| if *x {1} else {0}).sum();
                let mut messages = itertools::repeat_n(Message::Data(x), msg_count);
                for (enabled, sender) in output_flags.into_iter().zip_eq(self.senders.iter())
                {
                    if enabled {
                        // PANIC: we know next will work because we called repeat_n with
                        // the sum of all `true` vals
                        let msg = messages.next().unwrap();
                        sender.send(msg);
                    }
                }
            }
            x => {
                // repeat_n will clone for every iteration except the last
                // this gives us a small optimization on the common "1 receiver" case :)
                let messages = self
                    .senders
                    .iter_mut()
                    .zip(itertools::repeat_n(x, recipient_len));
                for (sender, elem) in messages {
                    sender.send(elem);
                }
            }
        };
    }
    /// Get the frontier on this Sender, i.e the timestamp of the largest
    /// Epoch sent with this sender or `None` if no Epoch has been sent with
    /// this sender yet
    #[inline]
    pub fn get_frontier(&self) -> &Option<T> {
        &self.frontier
    }

    pub(crate) fn receiver_count(&self) -> usize {
        self.senders.len()
    }
}

struct UpstreamState<K, V, T> {
    epoch: Option<T>,
    receiver: spsc::Receiver<Message<K, V, T>>,
}
impl<K, V, T> UpstreamState<K, V, T> {
    fn new(receiver: spsc::Receiver<Message<K, V, T>>) -> Self {
        Self {
            epoch: None,
            receiver,
        }
    }
}

/// Operator Input
pub struct Input<K, V, T> {
    /// Each receiver in this Vec is an inbound edge to the
    /// operator
    receivers: Vec<UpstreamState<K, V, T>>,
    // largest observed Epoch
    frontier: Option<T>,
}

impl<K, V, T> Input<K, V, T> {
    pub fn new_unlinked() -> Self {
        Self {
            receivers: Vec::new(),
            frontier: None,
        }
    }

    pub fn can_progress(&self) -> bool {
        self.receivers
            .iter()
            .any(|x| x.receiver.can_recv_unaligned())
    }

    #[inline]
    pub(crate) fn get_frontier(&self) -> &Option<T> {
        &self.frontier
    }
}

/// Small reducer hack, as we can't use iter::reduce because of ownership
fn merge_timestamps<'a, T: MaybeTime>(
    mut timestamps: impl Iterator<Item = &'a Option<T>>,
) -> Option<T> {
    let mut merged = timestamps.next()?.clone();
    for x in timestamps {
        if let Some(y) = x {
            merged = merged.and_then(|a| a.try_merge(y));
        } else {
            return None;
        }
    }
    merged
}

pub(crate) fn merge_receiver_groups<K, V, T: MaybeTime>(
    groups: Vec<Input<K, V, T>>,
) -> Input<K, V, T> {
    let frontier = merge_timestamps(groups.iter().map(|x| x.get_frontier()));
    let receivers: Vec<_> = groups.into_iter().flat_map(|x| x.receivers).collect();
    Input {
        receivers,
        frontier,
    }
}

impl<K, V, T> Input<K, V, T>
where
    T: MaybeTime,
{
    /// Receive a value. None if no value to receive or all Senders dropped.
    ///
    /// This method synchronizes barriers, i.e. if a channel is barred, it will
    /// not receive any messages from that channel until all channels are barred.
    /// Once all channels are barred, a single barrier will be emitted
    pub fn recv(&mut self) -> Option<Message<K, V, T>> {
        // TODO: This is left biased
        let next = self
            .receivers
            .iter()
            .enumerate()
            .find_map(|(i, x)| x.receiver.recv_unaligned().map(|msg| (msg, i)));
        match next {
            Some((msg, sender_idx)) => match msg {
                Message::Epoch(e) => {
                    // PANIC: We can unwrap because we got the idx from the iterator above
                    self.receivers.get_mut(sender_idx).unwrap().epoch = Some(e);
                    let merged = merge_timestamps(self.receivers.iter().map(|x| &x.epoch));
                    if let Some(m) = merged.as_ref() {
                        if self.frontier.as_ref().map_or(true, |frontier| frontier < m) {
                            self.frontier = Some(m.clone());
                        }
                    }
                    merged.map(|x| Message::Epoch(x.clone()))
                }
                x => Some(x),
            },
            None => {
                // there are multiple possibilities on why we did not get a next msg
                // 1. all messages are barriers, suspend markers or none
                // 2. self.receivers is empty
                // 3. all channels have barrier upcoming
                // 4. all channels have a suspend upcoming

                // 1 is most common so we check it first
                if self.receivers.iter().any(|x| x.receiver.is_empty()) {
                    return None;
                }
                // 2
                if self.receivers.len() == 0 {
                    return None;
                }
                // 3. all channels have barrier upcoming
                if self.receivers.iter().all(|x| {
                    x.receiver
                        .peek_apply(|y| matches!(y, Message::AbsBarrier(_)))
                        .unwrap_or(false)
                }) {
                    // take .last() to clear the barriers from all receivers
                    return self.receivers.iter().flat_map(|x| x.receiver.recv()).last();
                }
                // 4. all channels have a suspend upcoming
                if self.receivers.iter().all(|x| {
                    x.receiver
                        .peek_apply(|y| matches!(y, Message::SuspendMarker(_)))
                        .unwrap_or(false)
                }) {
                    // take .last() to clear the marker from all receivers
                    return self.receivers.iter().flat_map(|x| x.receiver.recv()).last();
                }
                // This could only be reached if there where mixed messages requiring alignement
                // like some receivers had a barrier and others a suspend marker which should
                // never happen
                unreachable!()
            }
        }
    }
}

trait RecvUnaligned<K, V, T> {
    /// Peek only those message types which do not
    /// require alignement, i.e. no barriers and suspend markers
    fn can_recv_unaligned(&self) -> bool;
    /// Receive only those message types which do not
    /// require alignement, i.e. no barriers and suspend markers
    fn recv_unaligned(&self) -> Option<Message<K, V, T>>;
}

impl<K, V, T> RecvUnaligned<K, V, T> for spsc::Receiver<Message<K, V, T>> {
    fn recv_unaligned(&self) -> Option<Message<K, V, T>> {
        self.can_recv_unaligned().then(|| self.recv()).flatten()
    }

    fn can_recv_unaligned(&self) -> bool {
        self.peek_apply(|next| match next {
            Message::AbsBarrier(_) => false,
            Message::SuspendMarker(_) => false,
            _ => true,
        })
        .unwrap_or(false)
    }
}

#[cfg(test)]
mod test {
    use crate::{
        snapshot::{Barrier, NoPersistence},
        types::{DataMessage, NoData, NoKey, NoTime, SuspendMarker},
    };

    use super::*;

    // /// Check we can clone a sender
    // #[test]
    // fn clone_sender() {
    //     let mut sender = Sender::new_unlinked(full_broadcast);
    //     let mut receiver = Receiver::new_unlinked();
    //     link(&mut sender, &mut receiver);

    //     let mut cloned = sender.clone();
    //     let msg = Message::Data(DataMessage {
    //         key: NoKey,
    //         value: "Hello",
    //         timestamp: NoTime,
    //     });
    //     cloned.send(msg.clone());

    //     assert!(matches!(
    //         receiver.recv(),
    //         Some(Message::Data(DataMessage {
    //             key: NoKey,
    //             value: "Hello",
    //             timestamp: NoTime
    //         }))
    //     ));

    //     sender.send(msg);
    //     assert!(matches!(
    //         receiver.recv(),
    //         Some(Message::Data(DataMessage {
    //             key: NoKey,
    //             value: "Hello",
    //             timestamp: NoTime
    //         }))
    //     ));
    //     assert_eq!(receiver.states.len(), 2);
    // }

    /// Check we only emit an epoch when it changes
    #[test]
    fn emit_epoch_on_change() {
        let mut sender: Output<NoKey, NoData, i32> = Output::new_unlinked(full_broadcast);
        let mut sender2: Output<NoKey, NoData, i32> = Output::new_unlinked(full_broadcast);
        let mut receiver = Input::new_unlinked();
        link(&mut sender, &mut receiver);
        link(&mut sender2, &mut receiver);

        sender.send(Message::Epoch(42));

        assert!(receiver.recv().is_none());
        sender2.send(Message::Epoch(15));
        assert!(matches!(receiver.recv(), Some(Message::Epoch(15))));
    }

    /// only issue a barrier once it is aligned
    #[test]
    fn aligns_barriers() {
        let mut sender: Output<NoKey, NoData, i32> = Output::new_unlinked(full_broadcast);
        let mut sender2: Output<NoKey, NoData, i32> = Output::new_unlinked(full_broadcast);
        let mut receiver = Input::new_unlinked();
        link(&mut sender, &mut receiver);
        link(&mut sender2, &mut receiver);

        sender.send(Message::AbsBarrier(Barrier::new(
            Box::<NoPersistence>::default(),
        )));

        let received = receiver.recv();
        assert!(received.is_none(), "{received:?}");
        sender2.send(Message::AbsBarrier(Barrier::new(
            Box::<NoPersistence>::default(),
        )));

        assert!(matches!(receiver.recv(), Some(Message::AbsBarrier(_))));
    }

    /// should buffer messages if the channels if barred
    #[test]
    fn buffer_on_barriers() {
        let mut sender: Output<NoKey, i32, NoTime> = Output::new_unlinked(full_broadcast);
        let mut sender2: Output<NoKey, i32, NoTime> = Output::new_unlinked(full_broadcast);
        let mut receiver = Input::new_unlinked();
        link(&mut sender, &mut receiver);
        link(&mut sender2, &mut receiver);

        sender.send(Message::AbsBarrier(Barrier::new(
            Box::<NoPersistence>::default(),
        )));

        sender.send(Message::Data(DataMessage::new(NoKey, 42, NoTime)));
        sender.send(Message::Data(DataMessage::new(NoKey, 177, NoTime)));

        sender2.send(Message::AbsBarrier(Barrier::new(
            Box::<NoPersistence>::default(),
        )));
        assert!(matches!(receiver.recv(), Some(Message::AbsBarrier(_))));

        let msg = receiver.recv();
        assert!(
            matches!(
                msg,
                Some(Message::Data(DataMessage {
                    key: _,
                    value: 42,
                    timestamp: _
                }))
            ),
            "{msg:?}"
        );
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
        let mut sender: Output<NoKey, NoData, i32> = Output::new_unlinked(full_broadcast);
        let mut sender2: Output<NoKey, NoData, i32> = Output::new_unlinked(full_broadcast);
        let mut receiver = Input::new_unlinked();
        link(&mut sender, &mut receiver);
        link(&mut sender2, &mut receiver);

        sender.send(Message::SuspendMarker(SuspendMarker::default()));

        let received = receiver.recv();
        assert!(received.is_none(), "{received:?}");
        sender2.send(Message::SuspendMarker(SuspendMarker::default()));

        assert!(matches!(receiver.recv(), Some(Message::SuspendMarker(_))));
    }

    /// Dropping a sender should remove its state from the receiver
    // #[test]
    // fn drop_sender() {
    //     let mut sender: Sender<NoKey, i32, NoTime> = Sender::new_unlinked(full_broadcast);
    //     let mut receiver = Receiver::new_unlinked();
    //     link(&mut sender, &mut receiver);
    //     sender.send(Message::Data(DataMessage::new(NoKey, 42, NoTime)));
    //     sender.send(Message::Data(DataMessage::new(NoKey, 177, NoTime)));
    //     drop(sender);

    //     assert!(matches!(
    //         receiver.recv(),
    //         Some(Message::Data(DataMessage {
    //             key: _,
    //             value: 42,
    //             timestamp: _
    //         }))
    //     ));
    //     assert!(matches!(
    //         receiver.recv(),
    //         Some(Message::Data(DataMessage {
    //             key: _,
    //             value: 177,
    //             timestamp: _
    //         }))
    //     ));
    //     receiver.recv();
    //     assert!(matches!(receiver.receivers.len(), 0));
    // }

    /// Check the accessor for the largest sent epoch (frontier)
    #[test]
    fn observe_frontier() {
        let mut sender: Output<NoKey, NoData, i32> = Output::new_unlinked(full_broadcast);
        let mut receiver = Input::new_unlinked();
        link(&mut sender, &mut receiver);

        assert_eq!(*sender.get_frontier(), None);
        // non-epoch messages should not influence this
        sender.send(Message::Data(DataMessage::new(NoKey, NoData, 1337)));
        assert_eq!(*sender.get_frontier(), None);

        sender.send(Message::Epoch(42));
        assert_eq!(*sender.get_frontier(), Some(42));
        sender.send(Message::Epoch(15));
        assert_eq!(*sender.get_frontier(), Some(42));
        sender.send(Message::Epoch(i32::MAX));
        assert_eq!(*sender.get_frontier(), Some(i32::MAX));
    }

    #[test]
    fn receiver_observe_frontier() {
        let mut sender1: Output<NoKey, NoData, i32> = Output::new_unlinked(full_broadcast);
        let mut sender2: Output<NoKey, NoData, i32> = Output::new_unlinked(full_broadcast);
        let mut receiver = Input::new_unlinked();
        link(&mut sender1, &mut receiver);
        link(&mut sender2, &mut receiver);

        sender1.send(Message::Epoch(42));
        // not yet aligned
        receiver.recv();
        assert_eq!(*receiver.get_frontier(), None);

        sender2.send(Message::Epoch(78));
        receiver.recv();
        assert_eq!(*receiver.get_frontier(), Some(42));

        sender1.send(Message::Epoch(1337));
        sender2.send(Message::Epoch(1337));
        receiver.recv();
        receiver.recv();
        assert_eq!(*receiver.get_frontier(), Some(1337));
    }

    #[test]
    fn merges_timestamps() {
        assert_eq!(merge_timestamps([None, Some(43)].iter()), None);
        assert_eq!(merge_timestamps([Some(42), Some(43)].iter()), Some(42));
        assert_eq!(
            merge_timestamps([Some(1337), Some(1337)].iter()),
            Some(1337)
        );
        assert_eq!(merge_timestamps::<i32>([None, None].iter()), None);
    }

    /// Should just discard messages
    #[test]
    fn sender_without_sink_discards() {
        let mut sender = Output::new_unlinked(full_broadcast);
        let elem = Rc::new("brox");
        // this should not panic
        sender.send(Message::Data(DataMessage::new("Beeble", elem.clone(), 42)));
        // if the sender had kept or sent the message somewhere this should panic
        Rc::try_unwrap(elem).unwrap();
    }

    //Test linking a cloned sender works
    // #[test]
    // fn link_cloned_sender() {
    //     let mut sender = Sender::new_unlinked(full_broadcast);
    //     let mut sender2 = sender.clone();
    //     let mut receiver = Receiver::new_unlinked();
    //     link(&mut sender2, &mut receiver);
    //     let msg = Message::Data(DataMessage::new("Beeble", 123, 42));

    //     // should have linked both the original and cloned sender
    //     sender.send(msg);
    //     let result = receiver.recv();
    //     assert!(result.is_some())
    // }
}
