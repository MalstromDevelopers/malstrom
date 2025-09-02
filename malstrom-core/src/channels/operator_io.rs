//! Local IO channels for stream operators. These input and output types are how operators
//! **on the same worker** communicate with each other.
//! Essentially these are the edges in the stream graph.
use super::spsc;
use crate::types::{MaybeTime, Message, OperatorPartitioner};
use itertools::Itertools;
use std::rc::Rc;

/// Operator Output
pub struct Output<K, V, T> {
    // Each sender in this Vec is essentially one outgoing
    // edge from the operator
    senders: Vec<spsc::Sender<Message<K, V, T>>>,
    #[allow(clippy::type_complexity)] // it's not thaaat complex
    partitioner: Rc<dyn OperatorPartitioner<K, V, T>>,
    frontier: Option<T>,
    suspended: bool,
}

impl<K, V, T> Output<K, V, T>
where
    K: Clone,
    V: Clone,
    T: Clone + MaybeTime,
{
    /// Create a new Sender with **no** associated Receiver
    /// Link a receiver with [link].
    pub(crate) fn new_unlinked(partitioner: impl OperatorPartitioner<K, V, T>) -> Self {
        Self {
            senders: Vec::new(),
            partitioner: Rc::new(partitioner),
            frontier: None,
            suspended: false,
        }
    }

    /// Send a value into this channel.
    /// Data messages are distributed as per the partioning function.
    ///
    /// System messages are always broadcasted.
    pub fn send(&mut self, msg: Message<K, V, T>) {
        debug_assert!(!self.suspended);
        if let Message::Epoch(e) = &msg {
            if self.frontier.as_ref().is_some_and(|x| e > x) || self.frontier.is_none() {
                self.frontier = Some(e.clone());
            }
        }
        let recipient_len = self.senders.len();
        let mut output_flags = vec![false; recipient_len];
        match msg {
            Message::Data(x) => {
                (self.partitioner)(&x, &mut output_flags);
                let msg_count = output_flags.iter().map(|x| if *x { 1 } else { 0 }).sum();
                let mut messages = itertools::repeat_n(Message::Data(x), msg_count);
                for (enabled, sender) in output_flags.into_iter().zip_eq(self.senders.iter()) {
                    if enabled {
                        // PANIC: we know next will work because we called repeat_n with
                        // the sum of all `true` vals
                        #[allow(clippy::unwrap_used)]
                        let msg = messages.next().unwrap();
                        sender.send(msg);
                    }
                }
            }
            x => {
                if matches!(x, Message::SuspendMarker(_)) {
                    self.suspended = true
                }
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

    /// Check if a [Message::SuspendMarker] has been sent into this output
    #[inline]
    pub(crate) fn is_suspended(&self) -> bool {
        self.suspended
    }
}

/// State of the upstream sender providing us messages
struct UpstreamState<K, V, T> {
    /// Most recent epoch the sender sent
    epoch: Option<T>,
    /// Receiver linked to Sender
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
    /// Create a new input which is not (yet) linked to any output
    pub(crate) fn new_unlinked() -> Self {
        Self {
            receivers: Vec::new(),
            frontier: None,
        }
    }

    /// Return true if this input is currently capable of receiving messages and progressing
    /// its inputs.
    /// Being capable does not necessarily mean there are any messages.
    pub(crate) fn can_progress(&self) -> bool {
        self.receivers
            .iter()
            .any(|x| x.receiver.can_recv_unaligned())
    }

    /// Get the frontier of this Input, i.e. the largest Epoch it has seen so far
    #[inline]
    pub(crate) fn get_frontier(&self) -> &Option<T> {
        &self.frontier
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
                    self.receivers
                        .get_mut(sender_idx)
                        .expect("Expected valid index")
                        .epoch = Some(e);
                    let merged = merge_timestamps(self.receivers.iter().map(|x| &x.epoch));
                    if let Some(m) = merged.as_ref() {
                        if self.frontier.as_ref().is_none_or(|frontier| frontier < m) {
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
                if self.receivers.is_empty() {
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
        self.peek_apply(|next| !matches!(next, Message::AbsBarrier(_) | Message::SuspendMarker(_)))
            .unwrap_or(false)
    }
}

// /// A simple partitioner, which will broadcast a value to all receivers
#[inline(always)]
pub(crate) fn full_broadcast<T>(_: &T, outputs: &mut [bool]) {
    outputs.fill(true);
}

/// Link a Sender and receiver together
pub(crate) fn link<K, V, T>(sender: &mut Output<K, V, T>, receiver: &mut Input<K, V, T>) {
    let (tx, rx) = spsc::unbounded();
    sender.senders.push(tx);
    receiver.receivers.push(UpstreamState::new(rx));
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

/// Mege multiple inputs into a single input. The new Input will be linked to all the original
/// upstream Outputs.
pub(crate) fn merge_inputs<K, V, T: MaybeTime>(groups: Vec<Input<K, V, T>>) -> Input<K, V, T> {
    let frontier = merge_timestamps(groups.iter().map(|x| x.get_frontier()));
    let receivers: Vec<_> = groups.into_iter().flat_map(|x| x.receivers).collect();
    Input {
        receivers,
        frontier,
    }
}

#[cfg(test)]
mod test {
    use crate::{
        snapshot::{Barrier, NoPersistence},
        types::{DataMessage, NoData, NoKey, NoTime, SuspendMarker},
    };

    use super::*;

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

        sender.send(Message::AbsBarrier(Barrier::new(Box::new(NoPersistence))));

        let received = receiver.recv();
        assert!(received.is_none(), "{received:?}");
        sender2.send(Message::AbsBarrier(Barrier::new(Box::new(NoPersistence))));

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

        sender.send(Message::AbsBarrier(Barrier::new(Box::new(NoPersistence))));

        sender.send(Message::Data(DataMessage::new(NoKey, 42, NoTime)));
        sender.send(Message::Data(DataMessage::new(NoKey, 177, NoTime)));

        sender2.send(Message::AbsBarrier(Barrier::new(Box::new(NoPersistence))));
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

    /// Output should be suspended after sending suspend marker
    #[test]
    fn output_suspended_after_marker() {
        let mut sender: Output<NoKey, NoData, NoTime> = Output::new_unlinked(full_broadcast);
        sender.send(Message::SuspendMarker(SuspendMarker::default()));
        assert!(sender.is_suspended())
    }
}
