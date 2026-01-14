//! Build contexts used by operators
use std::rc::Rc;

use indexmap::{IndexMap, IndexSet};
use itertools::Itertools;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::runtime::communication::Distributable;
use crate::runtime::{BiCommunicationClient, CommunicationClient, OperatorOperatorComm};
use crate::snapshot::{PersistenceClient, deserialize_state};
use crate::types::{OperatorId, WorkerId};

/// Build context which is injected into the builder function of an operator at computation graph
/// build time. This happens shortly before execution.
pub struct BuildContext {
    /// ID of this worker
    pub worker_id: WorkerId,
    /// ID of this operator
    pub operator_id: OperatorId,
    /// User given name of this operator
    pub operator_name: String,
    persistence_backend: Rc<dyn PersistenceClient>,
    // HACK: We need this in the ica tests
    pub(crate) communication: Rc<dyn OperatorOperatorComm>,
    worker_ids: IndexSet<WorkerId>,
}

impl BuildContext {
    pub(crate) fn new(
        worker_id: WorkerId,
        operator_id: OperatorId,
        name: String,
        persistence_backend: Rc<dyn PersistenceClient>,
        communication: Rc<dyn OperatorOperatorComm>,
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
    pub async fn load_state<S: Serialize + DeserializeOwned>(&self) -> Option<S> {
        self.persistence_backend
            .load(&self.operator_id)
            .map(deserialize_state)
    }

    /// Get the IDs of all workers (including this one) which are part of the cluster
    /// at build time.
    /// NOTE: Malstrom is designed to scale dynamically, so this information may become outdated
    /// at runtime
    pub fn get_worker_ids(&self) -> &IndexSet<WorkerId> {
        &self.worker_ids
    }

    /// Create a client for inter-worker communication
    pub fn create_communication_client<T: Distributable>(
        &mut self,
        other_worker: WorkerId,
    ) -> BiCommunicationClient<T> {
        CommunicationClient::new(
            other_worker,
            self.operator_id,
            Rc::clone(&self.communication),
        )
        .expect("Backend communication failure")
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

/// Build context sent by worker to operators, can be turned into [BuildContext]
#[derive(Clone)]
pub(crate) struct WorkerBuildContext {
    worker_id: WorkerId,
    persistence_backend: Rc<dyn PersistenceClient>,
    communication: Rc<dyn OperatorOperatorComm>,
    worker_ids: IndexSet<WorkerId>,
}

impl WorkerBuildContext {
    pub(crate) fn new(
        worker_id: WorkerId,
        persistence_backend: Rc<dyn PersistenceClient>,
        communication: Rc<dyn OperatorOperatorComm>,
        worker_ids: IndexSet<WorkerId>,
    ) -> Self {
        Self {
            worker_id,
            persistence_backend,
            communication,
            worker_ids,
        }
    }
}

impl WorkerBuildContext {
    /// Enriches this context with operator specific information and turns it
    /// into a full build context
    pub(crate) fn to_build_context(
        self,
        operator_id: OperatorId,
        operator_name: String,
    ) -> BuildContext {
        BuildContext {
            operator_id,
            operator_name,
            worker_id: self.worker_id,
            persistence_backend: self.persistence_backend,
            communication: self.communication,
            worker_ids: self.worker_ids,
        }
    }
}
