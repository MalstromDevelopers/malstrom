//! Runtime contexts used by operators
use std::rc::Rc;

use indexmap::{IndexMap, IndexSet};
use itertools::Itertools;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::runtime::OperatorOperatorComm;
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
}

impl OperatorContext {
    pub(crate) fn new(worker_id: WorkerId, operator_id: OperatorId) -> Self {
        Self {
            worker_id,
            operator_id,
        }
    }
}
