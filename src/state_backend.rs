use std::collections::HashMap;

use serde::de::DeserializeOwned;
use serde::Serialize;

pub trait State {}
pub trait PersistentState: State + Serialize + DeserializeOwned {}

pub trait PersistentStateBackend<T, P: PersistentState> {
    fn load(&self, worker_index: usize) -> Option<P>;
    fn persist(&self, worker_index: usize, state: &P);
}

struct DummyPersistentBackend<K,V> {
    hashmap: HashMap<K,V>,
    saved: bool,
    loaded: bool
}

impl<K,V> DummyPersistentBackend<K,V> {
    pub fn new() -> DummyPersistentBackend<K,V> {
        DummyPersistentBackend {
            hashmap: HashMap::new(),
            saved: false,
            loaded: false}
    }
}

impl<K,V> PersistentStateBackend<K,V> for DummyPersistentBackend<K,V> {
    fn load(&self, worker_index: usize) -> Option<V> {
        None
    }

    fn persist(&self, worker_index: usize, state: &V) {
        todo!()
    }
}