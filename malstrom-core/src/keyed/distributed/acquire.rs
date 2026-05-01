use std::{marker::PhantomData, rc::Rc};

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

use crate::{keyed::distributed::wire_message::WireAcquire, types::{OperatorId, distributable::Distributable}};


/// Acquire encapsulates state which has moved to the current worker from another worker due to
/// a reconfiguration
#[derive(Clone)]
pub struct Acquire<K> {
    inner: Rc<(K, IndexMap<OperatorId, Vec<u8>>)>
}

impl<K> Acquire<K> where K: Distributable {

    /// Take the moved state for a given order from this [Acquire]
    pub fn take_state<S: Distributable>(operator_id: &OperatorId) -> (K, S) {
        todo!()
    }
}

impl<K> From<WireAcquire<K>> for Acquire<K> {
    fn from(value: WireAcquire<K>) -> Self {
        let inner = Rc::new((value.key, value.collection));
        Self { inner }
    }
}