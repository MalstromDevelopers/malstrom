use std::collections::HashMap;
use std::hash::Hash;

use crate::stream::Data;

/// Trait to represent state backends which supports
/// key values
pub trait StateBackend<K, V>
where
    K: PartialEq + Eq + Hash,
    V: Data,
{
    fn set(&mut self, key: K, value: V);
    fn get(&self, key: K) -> Option<V>;
}

/// Hashmap based state backend
struct HashMapStateBackend<K, V>
where
    K: PartialEq + Eq + Hash,
    V: Data,
{
    map: HashMap<K, V>,
}

impl<K, V> StateBackend<K, V> for HashMapStateBackend<K, V>
where
    K: PartialEq + Eq + Hash,
    V: Data,
{
    /// Setter for keys
    fn set(&mut self, key: K, value: V) {
        self.map.insert(key, value);
    }

    // Getter for keys
    fn get(&self, key: K) -> Option<V> {
        self.map.get(&key).cloned()
    }
}
