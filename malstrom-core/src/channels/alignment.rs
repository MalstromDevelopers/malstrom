use std::hash::Hash;

use futures::{StreamExt, stream::FuturesUnordered};
use indexmap::{IndexMap, IndexSet};

use crate::channels::spsc;

use super::spsc::Receiver;

/// A group of [Receiver]s which will pause each receiver when the last message received
/// satisfies a given condition.
/// The receiver is unpaused once all receivers have met the condition.
/// Messages satisfying the condition are not immediatly emitted, but instead all emitted once
/// all receivers have met the condition. The order in which the paused messages are emitted is
/// **not specified**
pub(crate) struct AlignmentGroup<K, R: super::recv_trait::Receiver, F> {
    receivers: IndexMap<K, AlignedReceiver<R>>,
    condition: F,
}

struct AlignedReceiver<R: super::recv_trait::Receiver> {
    receiver: R,
    /// double duty as flag whether the receiver is paused and contains paused message
    paused: Option<R::Output>,
}

impl<K, R, F> AlignmentGroup<K, R, F>
where
    K: Hash + Eq,
    R: super::recv_trait::Receiver,
    F: Fn(&R::Output) -> bool,
{
    /// Create a new AlignmentGroup with the given receivers and condition function
    pub fn new(receivers: impl IntoIterator<Item = (K, R)>, condition: F) -> Self {
        let aligned_receivers = receivers
            .into_iter()
            .map(|(key, receiver)| {
                (
                    key,
                    AlignedReceiver {
                        receiver,
                        paused: None,
                    },
                )
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
            receivers: IndexMap::new(),
            condition,
        }
    }

    /// Add a new receiver to the AlignmentGroup
    pub fn insert(&mut self, key: K, receiver: R) {
        self.receivers.insert(
            key,
            AlignedReceiver {
                receiver,
                paused: None,
            },
        );
    }

    /// Remove a receiver from the [AlignmentGroup]
    pub fn remove(&mut self, key: &K) {
        let _ = self.receivers.swap_remove(key);
    }

    /// retain only the those keys where keep returns true
    pub fn retain(&mut self, mut keep: impl FnMut(&K) -> bool) {
        self.receivers.retain(|k, _| keep(k));
    }

    /// Get the set of keys in this alignmentgroup
    pub fn keys(&self) -> impl Iterator<Item = &K> {
        self.receivers.keys()
    }

    pub fn values(&self) -> impl Iterator<Item = &R> {
        self.receivers.values().map(|x| &x.receiver)
    }

    pub fn get_mut(&mut self, key: &K) -> Option<&mut R> {
        self.receivers.get_mut(key).map(|x| &mut x.receiver)
    }
}

impl<K, R, F> super::recv_trait::Receiver for AlignmentGroup<K, R, F>
where
    K: Clone,
    R: super::recv_trait::Receiver,
    F: Fn(&R::Output) -> bool,
{
    type Output = AlignedValue<K, R::Output>;

    async fn recv(&mut self) -> Self::Output {
        let mut recv_futures: FuturesUnordered<_> = self
            .receivers
            .iter_mut()
            .filter(|(_, x)| x.paused.is_none())
            .map(|(k, v)| async move {
                let msg = v.receiver.recv().await;
                (k, v, msg)
            })
            .collect();

        if recv_futures.is_empty() {
            std::future::pending::<()>().await;
        }

        loop {
            // TODO: left biased
            match recv_futures.next().await {
                Some((key, aligned_receiver, msg)) => {
                    if (self.condition)(&msg) {
                        aligned_receiver.paused = Some(msg);
                        continue;
                    }
                    return AlignedValue::Unaligned((key.clone(), msg));
                }
                // no unblocked futures or self.receivers is empty
                None => {
                    drop(recv_futures);
                    let aligned_values = self
                        .receivers
                        .iter_mut()
                        .map(|(key, x)| {
                            (
                                key.clone(),
                                x.paused.take().expect("Expected paused message"),
                            )
                        })
                        .collect();
                    return AlignedValue::Aligned(aligned_values);
                }
            }
        }
    }
}

pub(crate) enum AlignedValue<K, T> {
    /// Individual value of T, does not need alignment
    /// and index of channel this value came from
    Unaligned((K, T)),
    /// Multiple values which were aligned
    Aligned(Vec<(K, T)>),
}

#[cfg(test)]
mod tests {
    fn todo() {
        unimplemented!()
    }
}
