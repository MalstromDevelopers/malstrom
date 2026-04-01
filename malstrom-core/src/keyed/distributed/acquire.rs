use std::marker::PhantomData;

use crate::types::{OperatorId, distributable::Distributable};


/// Acquire encapsulates state which has moved to the current worker from another worker due to
/// a reconfiguration
pub struct Acquire<K> {
    _key_type: PhantomData<K>
}

impl<K> Acquire<K> where K: Distributable {

    /// Take the moved state for a given order from this [Acquire]
    pub fn take_state<S: Distributable>(operator_id: &OperatorId) -> (K, S) {
        todo!()
    }
}