//! Types and traits specific to keyed streams
use indexmap::IndexSet;
use serde::{de::DeserializeOwned, Serialize};

use crate::types::{Timestamp, Key, MaybeData, WorkerId};

/// Marker trait for functions which determine inter-worker routing
pub trait WorkerPartitioner<K>:
    for<'a> Fn(&K, &'a IndexSet<WorkerId>) -> &'a WorkerId + 'static
{
}
impl<K, U: for<'a> Fn(&K, &'a IndexSet<WorkerId>) -> &'a WorkerId + 'static> WorkerPartitioner<K>
    for U
{
}


// Marker trait for distributable keys, values or time
pub trait Distributable: Serialize + DeserializeOwned + 'static{}
impl<T: Serialize + DeserializeOwned + 'static> Distributable for T {}

/// Marker trait for distributable key
pub trait DistKey: Key + Distributable {}
impl<T: Key + Distributable> DistKey for T {}
/// Marker trait for distributable value
pub trait DistData: MaybeData + Distributable {}
impl<T: MaybeData + Distributable> DistData for T {}

pub trait DistTimestamp: Timestamp + Distributable {}
impl<T: Timestamp + Distributable> DistTimestamp for T {}

pub(super) type Version = u64;