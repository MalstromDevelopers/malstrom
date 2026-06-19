//! Build contexts used by operators
use std::rc::Rc;

use indexmap::{IndexMap, IndexSet};
use itertools::Itertools;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::runtime::LocalRuntime;

use crate::runtime::OperatorOperatorComm;
use crate::snapshot::{PersistenceClient, deserialize_state};
use crate::types::{OperatorId, WorkerId};

/// Build context which is injected into the builder function of an operator at computation graph
/// build time. This happens shortly before execution.
pub struct BuildContext {
    /// ID of this worker
    pub worker_id: WorkerId,
    /// ID of this operator
    pub operator_id: OperatorId,
    /// Runtime of this operator
    pub operator_rt: Rc<LocalRuntime>,
    /// User given name of this operator
    pub operator_name: String,
    /// Last completed configuration
    pub config_version: u64,

    persistence: Rc<dyn PersistenceClient>,
    communication: Rc<dyn OperatorOperatorComm>,
    worker_ids: IndexSet<WorkerId>,
}

impl BuildContext {
    pub(crate) fn new(
        worker_id: WorkerId,
        operator_id: OperatorId,
        operator_rt: Rc<LocalRuntime>,
        name: String,
        config_version: u64,
        persistence: Rc<dyn PersistenceClient>,
        communication: Rc<dyn OperatorOperatorComm>,
        worker_ids: IndexSet<WorkerId>,
    ) -> Self {
        Self {
            worker_id,
            operator_id,
            operator_rt,
            operator_name: name,
            config_version,
            persistence,
            communication,
            worker_ids,
        }
    }

    /// Load the persisted state for this operator.
    /// If no persisted state exists, this returns `None`
    pub async fn load_state<S: Serialize + DeserializeOwned>(&self) -> Option<S> {
        self.persistence
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

    pub(crate) fn get_communication(&self) -> Rc<dyn OperatorOperatorComm> {
        Rc::clone(&self.communication)
    }
}

/// Build context sent by worker to operators, can be turned into [BuildContext]
#[derive(Clone)]
pub(crate) struct WorkerBuildContext {
    worker_id: WorkerId,
    persistence: Rc<dyn PersistenceClient>,
    communication: Rc<dyn OperatorOperatorComm>,
    worker_ids: IndexSet<WorkerId>,
    config_version: u64,
    operator_rt: Rc<LocalRuntime>,
}

impl WorkerBuildContext {
    pub(crate) fn new(
        worker_id: WorkerId,
        persistence: Rc<dyn PersistenceClient>,
        communication: Rc<dyn OperatorOperatorComm>,
        worker_ids: IndexSet<WorkerId>,
        config_version: u64,
        operator_rt: Rc<LocalRuntime>,
    ) -> Self {
        Self {
            worker_id,
            persistence,
            communication,
            worker_ids,
            config_version,
            operator_rt,
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
            operator_rt: self.operator_rt,
            worker_id: self.worker_id,
            persistence: self.persistence,
            communication: self.communication,
            worker_ids: self.worker_ids,
            config_version: self.config_version,
        }
    }
}
