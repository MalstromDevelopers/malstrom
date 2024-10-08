//! Types and traits specific to keyed streams
use indexmap::IndexSet;
use serde::{Serialize};

use crate::{runtime::communication::Distributable, types::{Key, MaybeData, Timestamp, WorkerId}};
use std::fmt::Debug;

// /// Marker trait for functions which determine inter-worker routing
// pub trait WorkerPartitioner<K>:
//     for<'a> fn(&K, &'a IndexSet<WorkerId>) -> &'a WorkerId + 'static
// {
// }
// impl<K, U: for<'a> Fn(&K, &'a IndexSet<WorkerId>) -> &'a WorkerId + 'static> WorkerPartitioner<K>
//     for U
// {
// }
