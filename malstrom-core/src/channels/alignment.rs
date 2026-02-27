use futures::{StreamExt, stream::FuturesUnordered};

use crate::channels::spsc;

use super::spsc::Receiver;

/// A group of [Receiver]s which will pause each receiver when the last message received
/// satisfies a given condition.
/// The receiver is unpaused once all receivers have met the condition.
/// Messages satisfying the condition are not immediatly emitted, but instead all emitted once
/// all receivers have met the condition. The order in which the paused messages are emitted is
/// **not specified**
pub(crate) struct AlignmentGroup<R: super::recv_trait::Receiver, F> {
    receivers: Vec<AlignedReceiver<R>>,
    condition: F,
}

struct AlignedReceiver<R: super::recv_trait::Receiver> {
    receiver: R,
    /// double duty as flag whether the receiver is paused and contains paused message
    paused: Option<R::Output>
}

impl<R, F> AlignmentGroup<R, F>
where
    R: super::recv_trait::Receiver,
    F: Fn(&R::Output) -> bool,
{
    /// Create a new AlignmentGroup with the given receivers and condition function
    pub fn new(receivers: impl IntoIterator<Item = R>, condition: F) -> Self {
        let aligned_receivers = receivers.into_iter()
            .map(|receiver| AlignedReceiver {
                receiver,
                paused: None,
            })
            .collect();

        Self {
            receivers: aligned_receivers,
            condition,
        }
    }

    /// Create a new empty AlignmentGroup with the given condition function
    pub fn new_empty(condition: F) -> Self {
        Self {
            receivers: Vec::new(),
            condition,
        }
    }

    /// Add a new receiver to the AlignmentGroup
    pub fn push(&mut self, receiver: R) {
        self.receivers.push(AlignedReceiver {
            receiver,
            paused: None,
        });
    }
}

impl<R, F> super::recv_trait::Receiver  for AlignmentGroup<R, F>
where
    R: super::recv_trait::Receiver,
    F: Fn(&R::Output) -> bool {
    type Output = AlignedValue<R::Output>;

    async fn recv(&mut self) -> AlignedValue<R::Output> {
        let mut recv_futures: FuturesUnordered<_> = self.receivers.iter_mut()
        .filter(|x| x.paused.is_none())
        .enumerate()
        .map(|(i, x)| async move {(x.receiver.recv().await, x, i)})
        .collect();
        
        loop {
            // TODO: left biased
            match recv_futures.next().await {
                Some((msg, aligned_receiver, idx)) => {
                    if (self.condition)(&msg) {
                        aligned_receiver.paused = Some(msg);
                        continue;
                    }
                    return AlignedValue::Unaligned((msg, idx))
                },
                // no unblocked futures or self.receivers is empty
                None => {
                    drop(recv_futures);
                    let aligned_values = self.receivers
                    .iter_mut()
                    .map(|x| x.paused.take().expect("Expected paused message"))
                    .collect();
                    return AlignedValue::Aligned(aligned_values)
                },
            }
        }

    }
}

pub(crate) enum AlignedValue<T> {
    /// Individual value of T, does not need alignment
    /// and index of channel this value came from
    Unaligned((T, usize)),
    /// Multiple values which were aligned
    Aligned(Vec<T>)
}

#[cfg(test)]
mod tests {
    fn todo() {
        unimplemented!()
    }
}