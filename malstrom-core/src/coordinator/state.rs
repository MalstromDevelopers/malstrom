use std::sync::Arc;

use indexmap::{IndexMap, IndexSet};
use serde::{ser::SerializeMap, Deserialize, Serialize};
use tokio::task::JoinHandle;

use crate::{
    runtime::{communication::CoordinatorWorkerComm, CommunicationClient},
    snapshot::SnapshotVersion,
    types::WorkerId,
};

use super::{
    communication::{setup_comm, WorkerReceiver, WorkerSender},
    watchmap::WatchMap,
};

#[derive(PartialEq, Eq, Clone, Serialize, Deserialize)]
pub(super) enum WorkerPhase {
    Unknown,
    BuildComplete,
    Running,
    Snapshotting,
    Reconfiguring,
    Suspended,
    Completed,
}
impl Default for WorkerPhase {
    fn default() -> Self {
        Self::Unknown
    }
}

#[derive(Default, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub(super) struct WorkerState {
    pub phase: WorkerPhase,
    /// Last snapshot the worker has completed or None if not yet any
    pub snapshot_version: Option<SnapshotVersion>,
}

#[derive(Clone)]
pub struct CoordinatorState {
    pub(super) worker_states: WatchMap<WorkerId, WorkerState>,
    pub(super) active_workers: IndexMap<u64, WorkerSender>,
    /// Last reconfiguration the cluster has completed
    /// or None if no reconfiguration yet completed
    pub(super) config_version: Option<u64>,
}

impl CoordinatorState {
    pub async fn from_serialized<C>(ser: SerializableCoordinatorState, comm: &C) -> Self
    where
        C: CoordinatorWorkerComm,
    {
        let worker_states = WatchMap::from(ser.worker_states);
        let (active_workers, _recv_tasks) =
            setup_comm(comm, &ser.active_workers, &worker_states).await;
        Self {
            worker_states,
            active_workers,
            config_version: ser.config_version,
        }
    }

    pub async fn get_serializable(&self) -> SerializableCoordinatorState {
        SerializableCoordinatorState {
            worker_states: self.worker_states.clone_inner_map().await,
            active_workers: self.active_workers.keys().cloned().collect(),
            config_version: self.config_version.clone(),
        }
    }
}

#[derive(Default, Serialize, Deserialize)]
pub struct SerializableCoordinatorState {
    pub(super) worker_states: IndexMap<WorkerId, WorkerState>,
    pub(super) active_workers: IndexSet<WorkerId>,
    pub(super) config_version: Option<u64>,
}
