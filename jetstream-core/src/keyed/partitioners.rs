//! Partitioning functions for distributing a keyed stream across multiple workers.
use std::hash::{DefaultHasher, Hash, Hasher};


fn default_hash<T: Hash>(value: &T) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

/// Select a value from an Iterator of choices by applying [rendezvous hashing](https://en.wikipedia.org/wiki/Rendezvous_hashing).
/// Rendezvous hashing ensures minimal shuffling when the set of options changes
/// at the cost of being O(n) with n == options.len()
///
/// Returns None if the Iterator is empty
///
/// TODD: Add test
pub fn rendezvous_select<T: Hash, O: Hash>(
    value: &T,
    options: impl Iterator<Item = O>,
) -> Option<O> {
    let v_hash = default_hash(value);
    options
        .map(|x| (default_hash(&x).wrapping_add(v_hash), x))
        .max_by_key(|x| x.0)
        .map(|x| x.1)
}
