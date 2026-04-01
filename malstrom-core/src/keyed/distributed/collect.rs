use crate::types::{OperatorId, distributable::Distributable};


/// The Collect messages takes state from operators so it can be sent to another worker
pub struct Collect<K> {
    key: K
}

impl<K> Collect<K> where K: Distributable {

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