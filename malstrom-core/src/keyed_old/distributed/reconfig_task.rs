use std::{marker::PhantomData, task::Poll};

use futures::FutureExt;
use indexmap::{IndexMap, IndexSet};
use pin_project::pin_project;
use tokio::sync::{mpsc, oneshot};

use crate::{
    keyed::distributed::{Acquire, Collect},
    types::{Key, Kvt, OperatorId},
};

/// Handles local reconfiguration work i.e. interrogating for keys and collecting key states
enum OngoingReconfig<K> {
    /// Currently an Interrogation is running
    Interrogate(Interrogation<K>),
    /// Currently a Collect is running
    Collect(Collection<K>),
}

impl<K> OngoingReconfig<K>
where
    K: Key,
{
    /// Advance this task
    pub(crate) async fn advance(&mut self) -> ReconfigResult<K> {
        match self {
            OngoingReconfig::Interrogate(interrogate_fut) => {
                Self::advance_interrogate(interrogate_fut).await
            }
            OngoingReconfig::Collect(collect_fut) => {
                Self::advance_collect(collect_fut).await
            }
        }
    }

    async fn advance_interrogate(interrogate_fut: &mut Interrogation<K>) -> ReconfigResult<K> {
        let whitelist = interrogate_fut.await;
        // get first collect
        match Collection::new(whitelist.clone()) {
            Some((collect_fut, collect_msg)) => {
                let new_task = OngoingReconfig::Collect(collect_fut);
                ReconfigResult::InterrogateComplete((whitelist, collect_msg, new_task))
            }
            /// Whitelist was empty, there is no state needing collection
            None => ReconfigResult::NoCollect,
        }
    }

    async fn advance_collect(collect_fut: &mut Collection<K>) -> ReconfigResult<K> {
        match collect_fut.await {
            CollectionResult::NextCollect((next_fut, collect_msg, acquire)) => {
                let task = OngoingReconfig::Collect(next_fut);
                ReconfigResult::NextCollect((acquire, collect_msg, task))
            }
            CollectionResult::LastCollect(acquire) => ReconfigResult::LastCollect(acquire),
        }
    }
}

/// Result of advancing an [OngoingReconfig]
enum ReconfigResult<K> {
    /// Interrogation completed, returns whitelist and collect task
    InterrogateComplete((IndexSet<K>, Collect<K>, OngoingReconfig<K>)),
    /// Collect complete with new collect
    NextCollect((Acquire<K>, Collect<K>, OngoingReconfig<K>)),
    /// Collect complete with no new collect (done)
    LastCollect(Acquire<K>),
    /// Interrogation was concluded and yielded no keys which need to be collected
    NoCollect,
}

/// An ongoing interrogation for downstream keys, can be awaited and will return set of downstream
/// keys
#[pin_project]
struct Interrogation<K> {
    _key_type: PhantomData<K>,
    /// we get the interrogated keys back here once interrogation is complete
    #[pin]
    key_set_recv: oneshot::Receiver<IndexSet<K>>,
}

impl<K> Future for Interrogation<K> {
    type Output = IndexSet<K>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let mut this = self.project();
        this.key_set_recv
            .as_mut()
            .poll(cx)
            // PANIC: Can not happen because we send result in drop impl of Interrrogate
            .map(|res| res.expect("Interrogate must not be dropped without sending key set"))
    }
}

/// Ongoing collection of a key state. Can be awaited and completes once collection is complete
#[pin_project]
struct Collection<K> {
    /// State (wrapped in Option because of Future impl)
    state: Option<CollectionState<K>>,
    /// collected states for keys are received here
    #[pin]
    state_recv: mpsc::UnboundedReceiver<(OperatorId, Vec<u8>)>,

}

/// Values passed from one [Collection] to the next
struct CollectionState<K> {
    /// Key currently being collected
    current_key: K,
    /// Keys to be collected
    key_list: IndexSet<K>,
    /// collected states
    collected: IndexMap<OperatorId, Vec<u8>>,
}

impl<K> Collection<K> where K: Clone {
    /// Create a new Collect Future along with the corresponding Collect message to be sent out
    /// or none if whitelist is empty
    fn new(mut key_whitelist: IndexSet<K>) -> Option<(Self, Collect<K>)> {
        let key = match key_whitelist.pop() {
            Some(k) => k,
            None => return None,
        };
        let (collect_msg, collect_recv) = Collect::new(key.clone());
        let state = CollectionState {
            current_key: key,
            key_list: key_whitelist,
            collected: IndexMap::new()
        };
        let this = Self {
            state: Some(state),
            state_recv: collect_recv,
        };
        Some((this, collect_msg))
    }
}

impl<K> Future for Collection<K> where K: Key {
    type Output = CollectionResult<K>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let mut this = self.project();
        // Receiver poll takes care of waker
        match this.state_recv.as_mut().poll_recv(cx) {
            // all senders were dropped, which means collection for this key is done
            Poll::Ready(None) => {
                let state = this.state.take().expect("Must not be polled after completion");
                let acquire = Acquire::new(state.current_key, state.collected);

                // try getting next key to collect
                let next = Self::new(state.key_list);
                let out = match next {
                    Some((collect_fut, collect_msg)) => CollectionResult::NextCollect((collect_fut, collect_msg, acquire)),
                    // there is no next key to collect
                    None => CollectionResult::LastCollect(acquire),
                };
                Poll::Ready(out)
            },
            // we got a state, but there are still senders around
            Poll::Ready(Some((operator_id, key_state))) => {
                // state is only taken from Option in Poll::Ready branch
                this.state.as_mut().expect("Must not be polled after completion").collected.insert(operator_id, key_state);
                Poll::Pending
                
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Result of a [Collection]
enum CollectionResult<K> {
    /// Tuple of
    /// - next [Collection]
    /// - corresponding [Collect] message to be passed downstream
    /// - acquired state from this collection
    NextCollect((Collection<K>, Collect<K>, Acquire<K>)),
    /// This was the last collect, contains acquired state
    LastCollect(Acquire<K>),
}
