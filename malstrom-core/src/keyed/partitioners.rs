//! Partitioning functions for distributing a keyed stream across multiple workers.
use std::hash::{DefaultHasher, Hash, Hasher};

use indexmap::IndexSet;

/// Select a value from an Iterator of choices by applying [rendezvous hashing](https://en.wikipedia.org/wiki/Rendezvous_hashing).
/// Rendezvous hashing ensures minimal shuffling when the set of options changes
/// at the cost of being O(n) with n == options.len()
///
/// **PANIC:** if the set is empty
///
/// TODD: Add test
pub fn rendezvous_select<V: Hash, T: Hash + Copy>(value: &V, options: &IndexSet<T>) -> T {
    // TODO: DefaultHasher is not stable
    // see: https://doc.rust-lang.org/std/collections/hash_map/struct.DefaultHasher.html
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);

    options
        .iter()
        .map(|x| {
            let mut h = hasher.clone();
            x.hash(&mut h);
            (h.finish(), x)
        })
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
pub fn index_select<T: Copy>(i: &u64, s: &IndexSet<T>) -> T {
    let idx: usize = (*i).try_into().unwrap();
    *s.get_index(idx % s.len()).unwrap()
}
