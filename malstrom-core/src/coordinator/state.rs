use super::{
    communication::{setup_comm, SetupCommunicationError, WorkerSender},
    watchmap::WatchMap,
};
use crate::{
    runtime::communication::CoordinatorWorkerComm, snapshot::SnapshotVersion, types::WorkerId,
};
use indexmap::{IndexMap, IndexSet};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// What the worker is currently doing
#[derive(PartialEq, Eq, Clone, Serialize, Deserialize)]
pub(super) enum WorkerPhase {
    /// Not yet reported
    Unknown,
    /// Build completed, but execution not yet started
    BuildComplete,
    /// Execution started and running
    Running,
    /// Performing a snapshot
    Snapshotting,
    /// Performing a reconfiguration
    Reconfiguring,
    /// Suspend (not currently running)
    Suspended,
    /// Execution completed
    Completed,
}
impl Default for WorkerPhase {
    fn default() -> Self {
        Self::Unknown
    }
}

/// Current state of a worker
#[derive(Default, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub(super) struct WorkerState {
    /// What it's doing
    pub phase: WorkerPhase,
    /// Last snapshot the worker has completed or None if not yet any
    pub snapshot_version: Option<SnapshotVersion>,
}

#[derive(Clone)]
pub(crate) struct CoordinatorState {
    /// Contains state of all know workers, active or not
    pub(super) worker_states: WatchMap<WorkerId, WorkerState>,
    /// Map of senders to communicate with workers
    pub(super) active_workers: IndexMap<WorkerId, WorkerSender>,
    /// Last reconfiguration the cluster has completed
    /// or None if no reconfiguration yet completed
    pub(super) config_version: Option<u64>,
}

impl CoordinatorState {
    /// Load state from its serializable representation
    pub(crate) async fn from_serialized<C>(
        ser: SerializableCoordinatorState,
        comm: &C,
    ) -> Result<Self, FromSerializedError>
    where
        C: CoordinatorWorkerComm,
    {
        let worker_states = WatchMap::from(ser.worker_states);
        let (active_workers, _recv_tasks) =
            setup_comm(comm, &ser.active_workers, &worker_states).await?;
        Ok(Self {
            worker_states,
            active_workers,
            config_version: ser.config_version,
        })
    }

    /// Create a serializable version of the state by cloning.
    /// The serializable version, once created, is completely decoupled from the [CoordinatorState]
    /// i.e. updates are not reflected
    pub(crate) async fn get_serializable(&self) -> SerializableCoordinatorState {
        SerializableCoordinatorState {
            worker_states: self.worker_states.clone_inner_map().await,
            active_workers: self.active_workers.keys().cloned().collect(),
            config_version: self.config_version,
        }
    }
}

#[derive(Debug, Error)]
pub enum FromSerializedError {
    #[error("Error setting up communication")]
    SetupCommunication(#[from] SetupCommunicationError),
}

/// A serializable version of the [CoordinatorState]
#[derive(Default, Serialize, Deserialize)]
pub(crate) struct SerializableCoordinatorState {
    pub(super) worker_states: IndexMap<WorkerId, WorkerState>,
    pub(super) active_workers: IndexSet<WorkerId>,
    pub(super) config_version: Option<u64>,
}
