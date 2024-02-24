use std::rc::Rc;

/// Selective Broadcast:
///
/// Selective Broadcast channels are MPMC channels, where a partitioning function is used
/// to determine which receivers shall receive a value.
/// The value is copied as often as necessary, to ensure multiple receivers can receive it.
/// NOTE: This has absolutely NO guards against slow consumers. Slow consumers can build very
/// big queues with this channel.
use crossbeam;
use indexmap::IndexSet;
use itertools::{self, Itertools};

use crate::{DataMessage, Message, OperatorId, OperatorPartitioner, Scale};

struct BarrierReceiver<K, T, P>(crossbeam::channel::Receiver<Message<K, T, P>>, Option<P>);

impl<K, T, P> BarrierReceiver<K, T, P> {
    fn new(rx: crossbeam::channel::Receiver<Message<K, T, P>>) -> Self {
        Self(rx, None)
    }

    /// Receive only data, no barriers.
    /// If a barrier is the next element in the channel, return None
    fn receive_data(&mut self) -> Option<Message<K, T, P>> {
        if self.1.is_some() {
            return None;
        }
        match self.0.try_recv().ok() {
            x => x,
            Some(Message::AbsBarrier(b)) => {
                self.1 = Some(b);
                None
            }
            None => None,
        }
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn is_barred(&self) -> bool {
        self.1.is_some()
    }
    /// Remove any existing barrier from this channel
    fn take_barrier(&mut self) -> Option<Message<K, T, P>> {
        self.1.take().map(|x| Message::AbsBarrier(x))
    }
}

// /// A simple partitioner, which will broadcast a value to all receivers
pub fn full_broadcast<T>(_: &T, scale: Scale) -> IndexSet<OperatorId> {
    (0..scale).collect()
}

/// Link a Sender and receiver together
pub fn link<K, T, P>(sender: &mut Sender<K, T, P>, receiver: &mut Receiver<K, T, P>) {
    let receiver_inner = sender.subscribe_inner();
    receiver.0.push(receiver_inner);
}

/// Selective Broadcast Sender
#[derive(Clone)]
pub struct Sender<K, T, P> {
    // TOOD: We only have the partitioner in the Box to allow cloning
    // Which is only really needed in the snapshot conroller
    // Check if we can solve that another way
    senders: Vec<crossbeam::channel::Sender<Message<K, T, P>>>,
    #[allow(clippy::type_complexity)] // it's not thaaat complex
    partitioner: Rc<dyn OperatorPartitioner<K, T>>,
}

/// Selective Broadcast Receiver
pub struct Receiver<K, T, P>(Vec<BarrierReceiver<K, T, P>>);

impl<K, T, P> Sender<K, T, P>
where
    K: Clone,
    T: Clone,
    P: Clone,
{
    pub fn new_unlinked(partitioner: impl OperatorPartitioner<K, T>) -> Self {
        Self {
            senders: Vec::new(),
            partitioner: Rc::new(partitioner),
        }
    }

    /// Send a value into this channel.
    /// Data messages are distributed as per the partioning function
    /// System messages are always broadcasted
    pub fn send(&mut self, msg: Message<K, T, P>) {
        if self.senders.is_empty() {
            return;
        }
        let recipient_len = self.senders.len();
        match msg {
            Message::Data(x) => {
                let indices = (self.partitioner)(&x, recipient_len);
                for (sender, elem) in self
                    .senders
                    .iter_mut()
                    .zip(itertools::repeat_n(Message::Data(x), indices.len()))
                {
                    let _ = sender.send(elem);
                }
            }
            _ => {
                // repeat_n will clone for every iteration except the last
                // this gives us a small optimization on the common "1 receiver" case :)
                for (sender, elem) in self
                    .senders
                    .iter_mut()
                    .zip(itertools::repeat_n(msg, recipient_len))
                {
                    let _ = sender.send(elem);
                }
            }
        };
    }
}

impl<K, T, P> Sender<K, T, P> {
    /// Subrscribe to this sender
    pub fn subscribe(&mut self) -> Receiver<K, T, P> {
        let rx = self.subscribe_inner();
        Receiver(vec![rx])
    }

    fn subscribe_inner(&mut self) -> BarrierReceiver<K, T, P> {
        let (tx, rx) = crossbeam::channel::unbounded();
        self.senders.push(tx);
        BarrierReceiver::new(rx)
    }
}

// impl <T> Clone for Sender<T> {
//     /// Clone the sender: Cloning results in another Sender, which can send values
//     /// to the original senders Receivers. NOTE: This will clone, with all receivers
//     /// which are subrscribed AT THE POINT OF CLONING. If receivers on the original
//     /// Sender are added or dropped after cloning, the new Sender will not be affected
//     fn clone(&self) -> Self {
//         let func = self.partitioner.as_ref().clone();
//         Self { senders: self.senders.clone(), partitioner: Box::new(func)}
//     }
// }

impl<K, T, P> Receiver<K, T, P> {
    pub fn new_unlinked() -> Self {
        Self(Vec::new())
    }

    /// Receive a value. None if no value to receive or all Senders dropped.
    ///
    /// This method synchronizes barriers, i.e. if a channel is barred, it will
    /// not receive any messages from that channel until all channels are barred.
    /// Once all channels are barred, a single barrier will be emitted
    pub fn recv(&mut self) -> Option<Message<K, T, P>> {
        for r in self.0.iter_mut() {
            match r.receive_data() {
                Some(x) => return Some(x),
                None => continue,
            }
        }
        // We can reach this by three possibilities
        // 1. There are no receivers in the vec
        // 2. There is no data
        // 3. All channels are barred
        if self.0.iter().all(|x| x.is_barred()) {
            // PANIC: Unwrap is safe as we just asserted, that they are all barred
            self.0.iter_mut().map(|x| x.take_barrier().unwrap()).last()
        } else {
            None
        }
    }

    pub fn is_empty(&self) -> bool {
        self.0.iter().all(|r| r.is_empty())
    }
}
