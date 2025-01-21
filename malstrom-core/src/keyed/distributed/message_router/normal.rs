use indexmap::IndexSet;
use serde::{Deserialize, Serialize};
use tracing::info;

use super::super::types::*;
use crate::types::*;

/// The "normal" message router, i.e. the one used while there is no rescaling
/// configuration under way
#[derive(Debug, Serialize, Deserialize)]
pub struct NormalRouter {
    // the set of all workers in the cluster, excluding this one
    pub(super) worker_set: IndexSet<WorkerId>,
    pub(super) version: Version,
}

impl NormalRouter {
    /// Create a new router for the given worker set (including the local worker)
    /// and for the given version
    pub(super) fn new(worker_set: IndexSet<WorkerId>, version: Version) -> Self {
        info!("Creating normal router with set {:?}", worker_set);
        Self {
            worker_set,
            version,
        }
    }
    pub(super) fn route_message<K>(&self, key: &K, partitioner: WorkerPartitioner<K>) -> WorkerId {
        partitioner(key, &self.worker_set)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use indexmap::IndexSet;

    /// a partitioner that just uses the key as a wrapping index
    fn partiton_index(i: &usize, s: &IndexSet<WorkerId>) -> WorkerId {
        *s.get_index(i % s.len()).unwrap()
    }

    #[test]
    /// Check we return the target the
    fn give_correct_target() {
        let router = NormalRouter::new(IndexSet::from([0, 1]), 0);
        assert_eq!(0, router.route_message(&0, partiton_index));
        assert_eq!(1, router.route_message(&1, partiton_index));
        assert_eq!(1, router.route_message(&555, partiton_index));
    }
}
