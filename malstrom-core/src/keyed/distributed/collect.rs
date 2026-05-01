use indexmap::IndexMap;
use tokio::sync::oneshot;
use std::hash::Hash;

use crate::types::{OperatorId, distributable::Distributable};

/// The Collect messages takes state from operators so it can be sent to another worker
pub struct Collect<K> {
    key: K,
    backchannel: tokio::sync::mpsc::UnboundedSender<(OperatorId, Vec<u8>)>
}

impl<K> Collect<K> where K: Hash + Eq {

    pub(super) fn new(key: K) -> (Self, tokio::sync::mpsc::UnboundedReceiver<(OperatorId, Vec<u8>)>) {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        (Self { key, backchannel: tx }, rx)
    }

    /// Add a state for the [Collect]'s key. The operator MUST not use the state or a clone of it
    /// after giving it to this method.
    /// 
    /// The correct key can be obtained from [Collect::get_key]
    pub fn add_state<S: Distributable>(state: S) {
        todo!()
    }

    pub fn get_key(&self) -> &K {
        &self.key
    }
}
