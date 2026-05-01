use std::marker::PhantomData;

use crate::types::{Kvt, OperatorId, distributable::Distributable};


/// The Interrogate message is passed along a stream to identify which keys have associated state
pub struct Interrogate<K> {
    sender: tokio::sync::mpsc::UnboundedSender<K>,
    _key_type: PhantomData<K>
}

impl<K> Interrogate<K> where K: Distributable {

    pub(super) fn new() -> (Self, tokio::sync::mpsc::UnboundedReceiver<K>, ) {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        (Self{sender: tx, _key_type: PhantomData}, rx)
    }

    /// Inform this [Interrogate] about multiple keys for which this operator has state
    pub fn add_keys(&self, keys: impl IntoIterator<Item=K>) {
        for k in keys.into_iter() {
            self.sender.send(k).expect("Expected Interrogator to be alive")
        }
    }
}
