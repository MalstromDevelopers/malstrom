use std::{cell::RefCell, marker::PhantomData, rc::Rc};

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

use crate::{
    keyed::distributed::wire_message::WireAcquire, snapshot::deserialize_state, types::{OperatorId, distributable::Distributable}
};

/// Acquire encapsulates state which has moved to the current worker from another worker due to
/// a reconfiguration
#[derive(Clone)]
pub struct Acquire<K> {
    inner: Rc<RefCell<(K, IndexMap<OperatorId, Vec<u8>>)>>,
}

impl<K> Acquire<K>
where
    K: Distributable + Clone,
{
    /// Take the moved state for a given order from this [Acquire]
    pub fn take_state<S: Distributable>(&self, operator_id: &OperatorId) -> Option<(K, S)> {
        let mut inner = self.inner.borrow_mut();
        match inner.1.swap_remove(operator_id) {
            Some(state) => Some((inner.0.clone(), deserialize_state(state))),
            None => None
        }

    }
}

impl<K> From<WireAcquire<K>> for Acquire<K> {
    fn from(value: WireAcquire<K>) -> Self {
        let inner = Rc::new(RefCell::new((value.key, value.collection)));
        Self { inner }
    }
}
