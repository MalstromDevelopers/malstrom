//! Partitioning functions for distributing a keyed stream across multiple workers.
use std::hash::{DefaultHasher, Hash, Hasher};

use indexmap::IndexSet;

fn default_hash<T: Hash>(value: &T) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

/// Select a value from an Iterator of choices by applying [rendezvous hashing](https://en.wikipedia.org/wiki/Rendezvous_hashing).
/// Rendezvous hashing ensures minimal shuffling when the set of options changes
/// at the cost of being O(n) with n == options.len()
///
/// **PANIC:** if the set is empty
///
/// TODD: Add test
pub fn rendezvous_select<T: Hash + Copy>(value: &usize, options: &IndexSet<T>) -> T {
    let v_hash = default_hash(value);
    options
        .iter()
        .map(|x| (default_hash(&x).wrapping_add(v_hash), x))
        .max_by_key(|x| x.0)
        .map(|x| x.1)
        .expect("Collection not empty")
        .to_owned()
}

/// A partitioner which just uses the key as a wrapping index
/// on the set of available workers.
/// This is good because it is fast, but leads to **a lot** of data
/// shuffling if the compute cluster size changes.
///
/// If you plan on scaling dynamically, consider [rendezvous_select].
///
/// **PANIC:** if the set is empty
pub fn index_select<T: Copy>(i: &usize, s: &IndexSet<T>) -> T {
    *s.get_index(i % s.len()).unwrap()
}
