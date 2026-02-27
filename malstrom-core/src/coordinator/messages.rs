//! Typed messages which workers and coordinator may send to each other
use crate::types::WorkerId;
use indexmap::IndexSet;
use serde::{Deserialize, Serialize};

/// The Coordinator sends this to the Worker on startup
/// to give the worker the info it needs for building
#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct BuildInformation {
    /// Workers in cluster
    pub(crate) worker_set: IndexSet<WorkerId>,
    /// snapshot which the workers shall load
    /// or none if starting fresh
    pub(crate) resume_snapshot: Option<u64>,
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct StartBuild(pub(crate) BuildInformation);

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct StartExecution;

#[derive(Clone, Serialize, Deserialize)]
pub(crate) enum RuntimeMessage {
    Snapshot(u64),
    Reconfigure((IndexSet<WorkerId>, u64)),
    Suspend,
    ExecutionComplete,
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct ExecutionComplete;
