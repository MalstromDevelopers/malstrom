use std::rc::Rc;

/// Selective Broadcast:
///
/// Selective Broadcast channels are MPMC channels, where a partitioning function is used
/// to determine which receivers shall receive a value.
/// The value is copied as often as necessary, to ensure multiple receivers can receive it.
/// NOTE: This has absolutely NO guards against slow consumers. Slow consumers can build very
/// big queues with this channel.
use crossbeam;
use itertools;

use crate::snapshot::barrier::BarrierData;

struct BarrierReceiver<T, P>(crossbeam::channel::Receiver<BarrierData<T, P>>, Option<P>);

impl<T, P> BarrierReceiver<T, P> {
    fn new(rx: crossbeam::channel::Receiver<BarrierData<T, P>>) -> Self {
        Self(rx, None)
    }

    /// Receive only data, no barriers.
    /// If a barrier is the next element in the channel, return None
    fn receive_data(&mut self) -> Option<BarrierData<T, P>> {
        if self.1.is_some() {
            return None;
        }
        match self.0.try_recv().ok() {
            Some(BarrierData::Data(d)) => Some(BarrierData::Data(d)),
            Some(BarrierData::Load(l)) => Some(BarrierData::Load(l)),
            Some(BarrierData::Barrier(b)) => {
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
    fn take_barrier(&mut self) -> Option<BarrierData<T, P>> {
        self.1.take().map(|x| BarrierData::Barrier(x))
    }
}

/// A simple partitioner, which will broadcast a value to all receivers
pub fn full_broadcast<T>(_: &T, recv_cnt: usize) -> Vec<usize> {
    (0..recv_cnt).collect()
}

/// Link a Sender and receiver together
pub fn link<T, P>(sender: &mut Sender<T, P>, receiver: &mut Receiver<T, P>) {
    let receiver_inner = sender.subscribe_inner();
    receiver.0.push(receiver_inner);
}

/// Selective Broadcast Sender
#[derive(Clone)]
pub struct Sender<T, P> {
    senders: Vec<crossbeam::channel::Sender<BarrierData<T, P>>>,
    partitioner: Rc<dyn Fn(&T, usize) -> Vec<usize>>,
}

/// Selective Broadcast Receiver
pub struct Receiver<T, P>(Vec<BarrierReceiver<T, P>>);

impl<T, P> Sender<T, P>
where
    T: Clone,
    P: Clone,
{
    pub fn new_unlinked(partitioner: impl Fn(&T, usize) -> Vec<usize> + 'static) -> Self {
        Self {
            senders: Vec::new(),
            partitioner: Rc::new(partitioner),
        }
    }

    /// Send a value into this channel. The value will be distributed to receiver
    /// as per the result of the partitioning function
    pub fn send(&mut self, msg: BarrierData<T, P>) {
        if self.senders.len() == 0 {
            return;
        }
        let recv_idxs = match &msg {
            BarrierData::Data(x) => (self.partitioner)(&x, self.senders.len()),
            BarrierData::Barrier(_) => (0..self.senders.len()).into_iter().collect(),
            BarrierData::Load(_) => (0..self.senders.len()).into_iter().collect(),
        };
        let recv_idxs_len = recv_idxs.len();
        // repeat_n will clone for every iteration except the last
        // this gives us a small optimization on the common "1 receiver" case :)
        for (idx, elem) in recv_idxs
            .into_iter()
            .zip(itertools::repeat_n(msg, recv_idxs_len))
        {
            // SAFETY: We modul o the index with the sender length, so it can not be
            // out of bounds
            let sender = unsafe { self.senders.get_unchecked(idx % self.senders.len()) };
            match sender.send(elem) {
                Ok(_) => continue,
                // remove dropped receivers
                Err(_) => {
                    self.senders.remove(idx);
                    self.senders.shrink_to_fit()
                }
            }
        }
    }
}

impl<T, P> Sender<T, P> {
    /// Subrscribe to this sender
    pub fn subscribe(&mut self) -> Receiver<T, P> {
        let rx = self.subscribe_inner();
        Receiver(vec![rx])
    }

    fn subscribe_inner(&mut self) -> BarrierReceiver<T, P> {
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

impl<T, P> Receiver<T, P> {
    pub fn new_unlinked() -> Self {
        Self(Vec::new())
    }

    /// Receive a value. None if no value to receive or all Senders dropped.
    ///
    /// This method synchronizes barriers, i.e. if a channel is barred, it will
    /// not receive any messages from that channel until all channels are barred.
    /// Once all channels are barred, a single barrier will be emitted
    pub fn recv(&mut self) -> Option<BarrierData<T, P>> {
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
            self.0.iter_mut().map(|x| x.take_barrier().unwrap()).next()
        } else {
            None
        }
    }

    pub fn is_empty(&self) -> bool {
        self.0.iter().all(|r| r.is_empty())
    }
}
