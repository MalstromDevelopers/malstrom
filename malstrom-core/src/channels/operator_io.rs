//! Local IO channels for stream operators. These input and output types are how operators
//! **on the same worker** communicate with each other.
//! Essentially these are the edges in the stream graph.
use super::spsc;
use crate::{
    channels::{alignment::{AlignedValue, AlignmentGroup}, recv_trait::Receiver, signal::{Signal, SignalHandle}},
    snapshot::SnapshotBarrier,
    types::{Barrier, Kvt, MaybeTime, Message, OperatorId, OperatorPartitioner, SuspendMarker, Timestamp},
};
use futures::{FutureExt, StreamExt, TryFutureExt, stream::FuturesUnordered};
use itertools::Itertools;
use std::{rc::Rc, usize};
use tokio::sync::{oneshot, watch};

/// Operator Output
pub struct Output<M: Kvt> {
    // Each sender in this Vec is essentially one outgoing
    // edge from the operator
    senders: Vec<spsc::Sender<Message<M>>>,
    partitioner: Box<dyn OperatorPartitioner<M>>,
    frontier: Option<<M as Kvt>::Timestamp>,
    /// signal will be sent here if the Output gets closed,
    /// either because it has seen the MAX timestamp or because
    /// it was suspended
    closed_signal: watch::Sender<bool>,
}

impl<M: Kvt> Output<M> {
    /// Create a new Sender with **no** associated Receiver
    /// Link a receiver with [link].
    pub(crate) fn new_unlinked(partitioner: impl OperatorPartitioner<M>) -> Self {
        /// Allow NoTime type to indicate a final output
        /// even if send is never called on this output
        let finalized_signal = Signal::new(M::Timestamp::CHECK_FINISHED(&None));
        let this = Self {
            senders: Vec::new(),
            partitioner: Box::new(partitioner),
            frontier: None,
            closed_signal: watch::Sender::new(false),
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
        debug_assert!(!*self.closed_signal.borrow());
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
                if matches!(x, Message::AbsBarrier(Barrier::Suspend(_))) {
                    self.closed_signal.send(true);
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
            self.closed_signal.send(true);
        };
    }
    /// Get the frontier on this Sender, i.e the timestamp of the largest
    /// Epoch sent with this sender or `None` if no Epoch has been sent with
    /// this sender yet
    #[inline]
    pub fn get_frontier(&self) -> &Option<<M as Kvt>::Timestamp> {
        &self.frontier
    }

    pub(crate) fn get_closed_signal(&self) -> ClosedSignal {
        let sub = self.closed_signal.subscribe();
        ClosedSignal(sub)
    }
}

struct ClosedSignal(watch::Receiver<bool>);

impl ClosedSignal {
    pub(crate) async fn wait_for(&mut self) -> impl Future<Output = ()> {
        // can ignore result because Err just means Sender was dropped
        async {
            let _ = self.0.wait_for(|x| *x).await;
        }
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

/// Outer group for Barriers, inner group for SuspendMarkers
type BarrierAlign<M> = AlignmentGroup<OperatorId, spsc::Receiver<Message<M>>, fn(&Message<M>) -> bool>;

fn is_barrier<M: Kvt>(msg: &Message<M>) -> bool {matches!(msg, Message::AbsBarrier(_))}

/// Operator Input
pub struct Input<M: Kvt> {
    /// Highest epoch seen so far per inbound edge,
    frontiers: Vec<Option<M::Timestamp>>,
    /// last Epoch value we sent out
    last_epoch: Option<M::Timestamp>,
    receivers: BarrierAlign<M>,
}

impl<M: Kvt> Input<M> {
    /// Create a new input which is not (yet) linked to any output
    pub(crate) fn new_unlinked() -> Input<M> {
        let barrier_align = BarrierAlign::new_empty(is_barrier);
        Self {
            frontiers: Vec::new(),
            last_epoch: None,
            receivers: barrier_align,
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
        self.last_epoch.clone()
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
        loop {
            // We loop here just for the case where we get an epoch but can not emit it
            // because of inputs which are behind or because it would not advance the frontier
            let (key, msg) = match self.receivers.recv().await {
                AlignedValue::Unaligned((key, msg)) => (key, msg),
                AlignedValue::Aligned(mut items) => {
                    // does not matter which barrier we send, as long as they are aligned
                    // index also does not matter
                    items.pop().expect("Expected at least one receiver in Input")
                }
            };
            match msg {
                Message::Epoch(e) => {
                    self.frontiers[key as usize] = Some(e);
                    let merged = merge_timestamps(self.frontiers.iter());
                    // Only sent out if we would advance the frontier
                    // TODO: test
                    let out_epoch = match (self.last_epoch.as_ref(), merged) {
                        (None, Some(e)) => Some(e),
                        (Some(le), Some(me)) if me > *le => Some(me),
                        _ => None
                    };
                    if let Some(e) = out_epoch {
                        self.last_epoch = Some(e.clone());
                        return Message::Epoch(e);
                    }
                },
                x => return x
            }
        }
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
    let next_key = receiver.receivers.keys().last().unwrap_or(&0) + 1;
    receiver.receivers.insert(next_key, rx);
    receiver.frontiers.push(None);
}

/// Small reducer hack, as we can't use iter::reduce because of ownership
/// TODO: Move this somewhere else
pub(crate) fn merge_timestamps<'a, T: MaybeTime>(
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
        snapshot::{SnapshotBarrier, NoPersistence},
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

        sender.send(Message::AbsBarrier(SnapshotBarrier::new(Box::new(NoPersistence))));

        let received = receiver.recv();
        assert!(received.is_none(), "{received:?}");
        sender2.send(Message::AbsBarrier(SnapshotBarrier::new(Box::new(NoPersistence))));

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

        sender.send(Message::AbsBarrier(SnapshotBarrier::new(Box::new(NoPersistence))));

        sender.send(Message::Data(DataMessage::new(NoKey, 42, NoTime)));
        sender.send(Message::Data(DataMessage::new(NoKey, 177, NoTime)));

        sender2.send(Message::AbsBarrier(SnapshotBarrier::new(Box::new(NoPersistence))));
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
