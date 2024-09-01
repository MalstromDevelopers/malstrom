//! Key types for keyed streams

use std::hash::Hash;

/// Marker trait for stream keys
pub trait Key: Hash + Eq + PartialEq + Clone + 'static {}
impl<T: Hash + Eq + PartialEq + Clone + 'static> Key for T {}

/// Marker trait to denote streams that may or may not be keyed
pub trait MaybeKey: Clone + 'static {}
impl<T: Clone + 'static> MaybeKey for T {}

/// Indicates an unkeyed stream
#[derive(Clone, Debug)]
pub struct NoKey;
