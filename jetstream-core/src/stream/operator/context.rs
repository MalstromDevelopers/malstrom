//! Build and runtime contexts used by operators
use std::ops::Range;

use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::channels::selective_broadcast::{full_broadcast, Receiver, Sender};
use crate::runtime::communication::Distributable;
use crate::runtime::{CommunicationBackend, CommunicationClient};
use crate::snapshot::{deserialize_state, PersistenceClient};
use crate::types::MaybeTime;
use crate::types::{MaybeKey, OperatorId, OperatorPartitioner, WorkerId};

use crate::types::Data;

/// This is a type injected to logic function at runtime
/// and cotains context, whicht the logic generally can not change
/// but utilize
pub struct OperatorContext<'a> {
    pub worker_id: WorkerId,
    pub operator_id: OperatorId,
    pub(super) communication: &'a mut dyn CommunicationBackend,
}

impl<'a> OperatorContext<'a> {
    /// Create a client for inter-worker communication
    pub fn create_communication_client<T: Distributable>(
        &mut self,
        other_worker: WorkerId,
        other_operator: OperatorId,
    ) -> CommunicationClient<T> {
        CommunicationClient::new(
            other_worker,
            other_operator,
            self.operator_id,
            self.communication,
        )
        .unwrap()
    }
}

pub struct BuildContext<'a> {
    pub worker_id: WorkerId,
    pub operator_id: OperatorId,
    pub label: String,
    persistence_backend: Box<dyn PersistenceClient>, // TODO: use a mutable ref here
    communication: &'a mut dyn CommunicationBackend,
    worker_ids: Range<usize>,
}
impl<'a> BuildContext<'a> {
    pub(crate) fn new(
        worker_id: WorkerId,
        operator_id: OperatorId,
        label: String,
        persistence_backend: Box<dyn PersistenceClient>,
        communication: &'a mut dyn CommunicationBackend,
        worker_ids: Range<usize>,
    ) -> Self {
        Self {
            worker_id,
            operator_id,
            label,
            persistence_backend,
            communication,
            worker_ids,
        }
    }

    pub fn load_state<S: Serialize + DeserializeOwned>(&self) -> Option<S> {
        self.persistence_backend
            .load(&self.operator_id)
            .map(deserialize_state)
    }

    /// Get the IDs of all workers (including this one) which are part of the cluster
    /// at build time.
    /// NOTE: JetStream is designed to scale dynamically, so this information may become outdated
    /// at runtime
    pub fn get_worker_ids(&self) -> Range<WorkerId> {
        self.worker_ids.clone()
    }

    /// Create a client for inter-worker communication
    pub fn create_communication_client<T: Distributable>(
        &mut self,
        other_worker: WorkerId,
        other_operator: OperatorId,
    ) -> CommunicationClient<T> {
        CommunicationClient::new(
            other_worker,
            other_operator,
            self.operator_id,
            self.communication,
        )
        .unwrap()
    }
}
