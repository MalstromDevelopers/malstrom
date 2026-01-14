//! Runtime contexts used by operators
use std::rc::Rc;

use indexmap::{IndexMap, IndexSet};
use itertools::Itertools;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::runtime::communication::Distributable;
use crate::runtime::{BiCommunicationClient, CommunicationClient, OperatorOperatorComm};
use crate::snapshot::{PersistenceClient, deserialize_state};
use crate::types::{OperatorId, WorkerId};

/// This is a type injected to logic function at runtime
/// and cotains context, whicht the logic generally can not change
/// but utilize
pub struct OperatorContext {
    /// ID of this worker
    pub worker_id: WorkerId,
    /// ID of this operator
    pub operator_id: OperatorId,
    pub(super) communication: Rc<dyn OperatorOperatorComm>,
}

impl OperatorContext {
    pub(crate) fn new(
        worker_id: WorkerId,
        operator_id: OperatorId,
        communication: Rc<dyn OperatorOperatorComm>,
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
        BiCommunicationClient::new(
            other_worker,
            self.operator_id,
            Rc::clone(&self.communication),
        )
        .expect("Backend Communication error")
    }
}
