//! Local IO channels for stream operators. These input and output types are how operators
//! **on the same worker** communicate with each other.
//! Essentially these are the edges in the stream graph.
use super::spsc;
use crate::{
    channels::signal::{Signal, SignalHandle},
    snapshot::Barrier,
    types::{Kvt, MaybeTime, Message, OperatorPartitioner, SuspendMarker, Timestamp},
};
use futures::{FutureExt, StreamExt, stream::FuturesUnordered};
use itertools::Itertools;
use std::{rc::Rc, usize};
use tokio::sync::oneshot;

/// Operator Output
pub struct Output<M: Kvt> {
    // Each sender in this Vec is essentially one outgoing
    // edge from the operator
    senders: Vec<spsc::Sender<Message<M>>>,
    partitioner: Box<dyn OperatorPartitioner<M>>,
    frontier: Option<<M as Kvt>::Timestamp>,
    suspended: bool,
    finalized_signal: Signal,
}

impl<M: Kvt> Output<M> {
    /// Create a new Sender with **no** associated Receiver
    /// Link a receiver with [link].
    pub(crate) fn new_unlinked(partitioner: impl OperatorPartitioner<M>) -> Self {
        let this = Self {
            senders: Vec::new(),
            partitioner: Box::new(partitioner),
            frontier: None,
            suspended: false,
            /// signal to listen for finished input (last epoch received or NoTime)
            finalized_signal: Signal::new(),
        };
        /// Allow NoTime type to indicate a final output
        /// even if send is never called on this output
        if M::Timestamp::CHECK_FINISHED(&None) {
            this.finalized_signal.send();
        };

        this
    }

    /// Send a value into this channel.
    /// Data messages are distributed as per the partioning function.
    ///
    /// System messages are always broadcasted.
    pub async fn send(&mut self, msg: Message<M>)
    where
        M: Clone,
    {
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
                        sender.send(msg).await;
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
                    sender.send(elem).await;
                }
            }
        };
        if M::Timestamp::CHECK_FINISHED(&self.frontier) {
            self.finalized_signal.send();
        };
    }
    /// Get the frontier on this Sender, i.e the timestamp of the largest
    /// Epoch sent with this sender or `None` if no Epoch has been sent with
    /// this sender yet
    #[inline]
    pub fn get_frontier(&self) -> &Option<<M as Kvt>::Timestamp> {
        &self.frontier
    }

    /// Check if a [Message::SuspendMarker] has been sent into this output
    #[inline]
    pub(crate) fn is_suspended(&self) -> bool {
        self.suspended
    }
    
    pub(crate) fn get_finalized_handle(&self) -> SignalHandle {
        self.finalized_signal.handle()
    }
}

#[derive(Default)]
pub(crate) struct RootOutput {
    senders: Vec<spsc::Sender<Message<()>>>,
}

impl RootOutput {
    // send a system message, this method is not async to allow sending
    // from a different or no runtime
    pub(crate) fn send_system(&mut self, msg: Message<()>) {
        for s in self.senders.iter() {
            s.force_send(msg.clone())
        }
    }
}

/// State of the upstream sender providing us messages
#[derive(Default)]
struct UpstreamState<M: Kvt> {
    /// Most recent epoch the sender sent
    epoch: Option<M::Timestamp>,
    /// Barrier currently waiting for alignment
    barred: bool,
    /// Susepend currently waiting for alignment
    suspended: bool,
}
impl<M: Kvt> UpstreamState<M> {
    fn new() -> Self {
        Self {
            epoch: None,
            barred: false,
            suspended: false,
        }
    }
}

/// Operator Input
pub struct Input<M: Kvt> {
    /// Each receiver in this Vec is an inbound edge to the
    /// operator
    states: Vec<UpstreamState<M>>,
    receivers: Vec<spsc::Receiver<Message<M>>>,
}

impl<M: Kvt> Input<M> {
    /// Create a new input which is not (yet) linked to any output
    pub(crate) fn new_unlinked() -> Input<M> {
        Self {
            states: Vec::new(),
            receivers: Vec::new(),
        }
    }
}

impl<M: Kvt> Input<M>
where
    M::Timestamp: MaybeTime,
{
    /// Get the frontier of this Input, i.e. the smallest Epoch currently merged
    #[inline]
    pub(crate) fn get_frontier(&self) -> Option<M::Timestamp> {
        merge_timestamps(self.states.iter().map(|x| &x.epoch))
    }
}

impl<M: Kvt> Input<M>
where
    <M as Kvt>::Timestamp: MaybeTime,
{
    /// Receive a value
    ///
    /// This method synchronizes barriers, i.e. if a channel is barred, it will
    /// not receive any messages from that channel until all channels are barred.
    /// Once all channels are barred, a single barrier will be emitted
    pub async fn recv(&mut self) -> Message<M> {
        // get all non-barred non-suspended links
        let ready_receivers = self.states.iter().map(|x| !x.barred && !x.suspended);
        let receivers = self
            .receivers
            .iter()
            .zip_eq(ready_receivers)
            .filter_map(|(recv, mask)| mask.then_some(recv))
            .enumerate();

        let mut unaligned_receivers: FuturesUnordered<_> = receivers
            .map(async |(i, receiver)| {
                let msg = receiver.recv().await;
                (i, msg)
            })
            .collect();

        // TODO: Ensure the Input is linked via the typesystem
        while let (i, msg) = unaligned_receivers
            .next()
            .await
            .expect("At least one receiver")
        {
            // get the first messag we can emit
            // PANIC: We can unwrap because we got the idx from the iterator above
            let state = self.states.get_mut(i).unwrap();
            match msg {
                Message::Epoch(e) => {
                    state.epoch = Some(e);
                    let merged = merge_timestamps(self.states.iter().map(|x| &x.epoch));
                    if let Some(m) = merged {
                        return Message::Epoch(m);
                    }
                }
                Message::AbsBarrier(barrier) => {
                    state.barred = true;
                    if self.states.iter().all(|x| x.barred) {
                        for st in self.states.iter_mut() {
                            st.barred = false;
                        }
                        return Message::AbsBarrier(barrier);
                    }
                }
                Message::SuspendMarker(suspend) => {
                    state.suspended = true;
                    if self.states.iter().all(|x| x.suspended) {
                        for st in self.states.iter_mut() {
                            st.suspended = false;
                        }
                        return Message::SuspendMarker(suspend);
                    }
                }
                x => return x,
            }
        }
        // if we reached heard it can be because:
        // 1. we got an epoch, but we can not issue it because the other inputs
        //    are behind
        // 2. We got a barrier, but we are still waiting for the barrier on other
        //    inputs
        // 3. We got a suspend marker but are still waiting for the marker on other
        //    inputs
        //
        // In all these cases just receiving again will solve the issue
        drop(unaligned_receivers);
        self.recv().await
    }
}

// /// A simple partitioner, which will broadcast a value to all receivers
#[inline(always)]
pub(crate) fn full_broadcast<T>(_: &T, outputs: &mut [bool]) {
    outputs.fill(true);
}

/// Link a Sender and receiver together
pub(crate) fn link<M: Kvt>(sender: &mut Output<M>, receiver: &mut Input<M>) {
    let (tx, rx) = spsc::unbounded();
    sender.senders.push(tx);
    receiver.receivers.push(rx);
    receiver.states.push(UpstreamState::new());
}

/// Link a Sender and receiver together
pub(crate) fn link_root(sender: &mut RootOutput, receiver: &mut Input<()>) {
    let (tx, rx) = spsc::unbounded();
    sender.senders.push(tx);
    receiver.receivers.push(rx);
    receiver.states.push(UpstreamState::new());
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
        let mut sender: Output<(NoKey, NoData, i32)> = Output::new_unlinked(full_broadcast);
        let mut sender2: Output<(NoKey, NoData, i32)> = Output::new_unlinked(full_broadcast);
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
        let mut sender: Output<(NoKey, NoData, i32)> = Output::new_unlinked(full_broadcast);
        let mut sender2: Output<(NoKey, NoData, i32)> = Output::new_unlinked(full_broadcast);
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
        let mut sender: Output<(NoKey, i32, NoTime)> = Output::new_unlinked(full_broadcast);
        let mut sender2: Output<(NoKey, i32, NoTime)> = Output::new_unlinked(full_broadcast);
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
        let mut sender: Output<(NoKey, NoData, i32)> = Output::new_unlinked(full_broadcast);
        let mut sender2: Output<(NoKey, NoData, i32)> = Output::new_unlinked(full_broadcast);
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
        let mut sender: Output<(NoKey, NoData, i32)> = Output::new_unlinked(full_broadcast);
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
        let mut sender1: Output<(NoKey, NoData, i32)> = Output::new_unlinked(full_broadcast);
        let mut sender2: Output<(NoKey, NoData, i32)> = Output::new_unlinked(full_broadcast);
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
        let mut sender: Output<(&str, Rc<&str>, i32)> = Output::new_unlinked(full_broadcast);
        let elem = Rc::new("brox");
        // this should not panic
        sender.send(Message::Data(DataMessage::new("Beeble", elem.clone(), 42)));
        // if the sender had kept or sent the message somewhere this should panic
        Rc::try_unwrap(elem).unwrap();
    }

    /// Output should be suspended after sending suspend marker
    #[test]
    fn output_suspended_after_marker() {
        let mut sender: Output<(NoKey, NoData, NoTime)> = Output::new_unlinked(full_broadcast);
        sender.send(Message::SuspendMarker(SuspendMarker::default()));
        assert!(sender.is_suspended())
    }
}
