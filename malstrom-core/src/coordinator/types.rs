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

/// Sent by Worker
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum WorkerMessage {
    /// DAG build completed
    BuildComplete,
    /// Dataflow execution started
    ExecutionStarted,
    /// Snapshoting started
    SnapshotStarted,
    /// Sent by worker to indicate snapshot completion
    SnapshotComplete(u64),
    /// Reconfiguration (rescale) started
    ReconfigurationStarted,
    /// Reconfiguration completed
    ReconfigureComplete(u64),
    /// Execution completed
    ExecutionComplete,
    /// Last message sent by worker before suspending
    SuspendComplete,
    /// worker was removed due to reconfiguration
    Removed,
}

/// Sent by coordinator
#[derive(Clone, Serialize, Deserialize)]
pub(crate) enum CoordinationMessage {
    /// Instructing worker to start build
    StartBuild(BuildInformation),
    /// Instruct worker to start execution
    StartExecution,
    /// Instruct worker to perform snapshot
    Snapshot(u64),
    /// Instruct workert to reconfigure/rescale to the new worker set. Workers should advance
    /// to the new config version
    Reconfigure((IndexSet<WorkerId>, u64)),
    /// Instruct worker to suspend execution
    Suspend,
}
