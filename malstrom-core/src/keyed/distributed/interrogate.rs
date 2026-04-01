use std::marker::PhantomData;

use crate::types::{OperatorId, distributable::Distributable};


/// The Interrogate message is passed along a stream to identify which keys have associated state
pub struct Interrogate<K> {
    _key_type: PhantomData<K>
}

impl<K> Interrogate<K> where K: Distributable {

    /// Inform this [Interrogate] about multiple keys for which this operator has state
    pub fn add_keys(keys: impl IntoIterator<Item=K>) {
        todo!()
    }
}