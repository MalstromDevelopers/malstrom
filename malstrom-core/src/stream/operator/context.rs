//! Build and runtime contexts used by operators
use std::rc::Rc;

use indexmap::{IndexMap, IndexSet};
use itertools::Itertools;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::errorhandling::MalstromFatal;
use crate::runtime::communication::Distributable;
use crate::runtime::{BiCommunicationClient, CommunicationClient, OperatorOperatorComm};
use crate::snapshot::{deserialize_state, PersistenceClient};
use crate::types::{OperatorId, WorkerId};

/// This is a type injected to logic function at runtime
/// and cotains context, whicht the logic generally can not change
/// but utilize
pub struct OperatorContext<'a> {
    /// ID of this worker
    pub worker_id: WorkerId,
    /// ID of this operator
    pub operator_id: OperatorId,
    pub(super) communication: &'a mut dyn OperatorOperatorComm,
}

#[allow(clippy::needless_lifetimes)] // elision does not work as clippy suggests here
impl<'a> OperatorContext<'a> {
    #[cfg(test)]
    pub(crate) fn new(
        worker_id: WorkerId,
        operator_id: OperatorId,
        communication: &'a mut dyn OperatorOperatorComm,
    ) -> Self {
        Self {
            worker_id,
            operator_id,
            communication,
        }
    }

    /// Create a client for inter-worker communication
    ///
    /// PANIC: This function panics if the given WorkerID is the ID of the worker it is called on.
    pub fn create_communication_client<T: Distributable>(
        &self,
        other_worker: WorkerId,
    ) -> BiCommunicationClient<T> {
        // Assert is kinda ugly here, but this situation is a programming error
        assert!(other_worker != self.worker_id);
        BiCommunicationClient::new(other_worker, self.operator_id, self.communication)
            .malstrom_fatal()
    }
}

/// Build context which is injected into the builder function of an operator at computation graph
/// build time. This happens shortly before execution.
pub struct BuildContext<'a> {
    /// ID of this worker
    pub worker_id: WorkerId,
    /// ID of this operator
    pub operator_id: OperatorId,
    /// User given name of this operator
    pub operator_name: String,
    persistence_backend: Rc<dyn PersistenceClient>,
    // HACK: We need this in the ica tests
    pub(crate) communication: &'a mut dyn OperatorOperatorComm,
    worker_ids: IndexSet<WorkerId>,
}
impl<'a> BuildContext<'a> {
    pub(crate) fn new(
        worker_id: WorkerId,
        operator_id: OperatorId,
        name: String,
        persistence_backend: Rc<dyn PersistenceClient>,
        communication: &'a mut dyn OperatorOperatorComm,
        worker_ids: IndexSet<WorkerId>,
    ) -> Self {
        Self {
            worker_id,
            operator_id,
            operator_name: name,
            persistence_backend,
            communication,
            worker_ids,
        }
    }

    /// Load the persisted state for this operator.
    /// If no persisted state exists, this returns `None`
    pub fn load_state<S: Serialize + DeserializeOwned>(&self) -> Option<S> {
        self.persistence_backend
            .load(&self.operator_id)
            .map(deserialize_state)
    }

    /// Get the IDs of all workers (including this one) which are part of the cluster
    /// at build time.
    /// NOTE: JetStream is designed to scale dynamically, so this information may become outdated
    /// at runtime
    pub fn get_worker_ids(&self) -> &IndexSet<WorkerId> {
        &self.worker_ids
    }

    /// Create a client for inter-worker communication
    pub fn create_communication_client<T: Distributable>(
        &mut self,
        other_worker: WorkerId,
    ) -> BiCommunicationClient<T> {
        CommunicationClient::new(other_worker, self.operator_id, self.communication)
            .malstrom_fatal()
    }

    /// Create clients for all workers active at build_time
    pub fn create_all_communication_clients<T: Distributable>(
        &mut self,
    ) -> IndexMap<WorkerId, BiCommunicationClient<T>> {
        let other_workers = self
            .get_worker_ids()
            .into_iter()
            .filter(|wid| **wid != self.worker_id)
            .cloned()
            .collect_vec();
        other_workers
            .into_iter()
            .map(|wid| (wid, self.create_communication_client(wid)))
            .collect()
    }
}
