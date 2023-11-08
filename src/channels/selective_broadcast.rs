use itertools;
use crossbeam;

/// Selective Broadcast:
/// 
/// Selective Broadcast channels are MPMC channels, where a partitioning function is used
/// to determine which receivers shall receive a value.
/// The value is copied as often as necessary, to ensure multiple receivers can receive it.
/// NOTE: This has absolutely NO guards against slow consumers. Slow consumers can build very
/// big queues with this channel.
pub fn unbounded<T >(partitioner: impl Fn(&T, usize) -> Vec<usize> + 'static) -> (Sender<T>, Receiver<T>) {
    let (tx, rx) = crossbeam::channel::unbounded::<T>();
    (
        Sender{senders: vec![tx], partitioner: Box::new(partitioner)},
        Receiver(vec![rx])
    )
}


/// A simple partitioner, which will broadcast a value to all receivers
pub fn full_broadcast<T>(_: &T, recv_cnt: usize) -> Vec<usize> {
    (0..recv_cnt).collect()
}

/// Link a Sender and receiver together
pub fn link<T>(sender: &mut Sender<T>, receiver: &mut Receiver<T>) {
    let receiver_inner = sender.subscribe_inner();
    receiver.0.push(receiver_inner);
}

/// Selective Broadcast Sender
pub struct Sender<T> {
    senders: Vec<crossbeam::channel::Sender<T>>,
    partitioner: Box<dyn Fn(&T, usize) -> Vec<usize>>
}
/// Selective Broadcast Receiver
pub struct Receiver<T>(Vec<crossbeam::channel::Receiver<T>>);

impl <T> Sender<T> where T: Clone {

    pub fn new_unlinked(partitioner: impl Fn(&T, usize) -> Vec<usize> + 'static) -> Self {
        Self { senders: Vec::new(), partitioner: Box::new(partitioner) }
    }

    /// Send a value into this channel. The value will be distributed to receiver
    /// as per the result of the partitioning function
    pub fn send(&mut self, msg: T) {
        let recv_idxs = (self.partitioner)(&msg, self.senders.len());
        let recv_idxs_len = recv_idxs.len();

        // repeat_n will clone for every iteration except the last
        // this gives us a small optimization on the common "1 receiver" case :)
        for (idx, elem) in recv_idxs.into_iter().zip(itertools::repeat_n(msg, recv_idxs_len)) {
            // SAFETY: We modul o the index with the sender length, so it can not be
            // out of bounds
            let sender = unsafe {self.senders.get_unchecked(idx % self.senders.len())};
            match sender.send(elem) {
                Ok(_) => continue,
                // remove dropped receivers
                Err(_) => {self.senders.remove(idx); self.senders.shrink_to_fit()}
            }
        }
    }
}

impl <T> Sender<T> {
    /// Subrscribe to this sender
    pub fn subscribe(&mut self) -> Receiver<T> {
        let rx = self.subscribe_inner();
        Receiver(vec![rx])
    }

    fn subscribe_inner(&mut self) -> crossbeam::channel::Receiver<T> {
        let (tx, rx) = crossbeam::channel::unbounded();
        self.senders.push(tx);
        rx
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

impl<T> Receiver<T> {

    pub fn new_unlinked() -> Self {
        Self(Vec::new())
    }

    /// Receive a value. None if no value to receive or all Senders dropped.
    pub fn recv(&self) -> Option<T> {
        for r in self.0.iter() {
            match r.recv().ok() {
                Some(x) => return Some(x),
                None => continue
            }
        };
        None
    }

    pub fn is_empty(&self) -> bool {
        self.0.iter().all(|r| r.is_empty())
    }
}

