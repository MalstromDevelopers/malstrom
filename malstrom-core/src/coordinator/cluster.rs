use futures::future::join_all;
use indexmap::{IndexMap, IndexSet};
use serde::{Deserialize, Serialize};

use crate::{
    coordinator::{COORDINATOR_ID, messages::*},
    runtime::communication::{CoordinatorClient, WorkerCoordinatorComm},
    snapshot::{PersistenceBackend, PersistenceClient, SnapshotVersion, deserialize_state},
    types::WorkerId,
};

/// Current state of a worker
#[derive(Default, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub(super) struct WorkerState {
    /// What it's doing
    pub phase: WorkerPhase,
    /// Last snapshot the worker has completed or None if not yet any
    pub snapshot_version: Option<SnapshotVersion>,
}

pub(crate) struct ClusterHandle {
    /// Contains state of all known workers, active or not
    pub(super) workers: IndexMap<WorkerId, (WorkerState, CoordinatorClient)>,
    /// Last reconfiguration the cluster has completed
    /// or None if no reconfiguration yet completed
    pub(super) config_version: Option<u64>,
    /// Last snapshot the coordinator has completed or None if not yet any
    pub snapshot_version: Option<SnapshotVersion>,
}

impl ClusterHandle {
    /// Create a serializable version of the state by cloning.
    /// The serializable version, once created, is completely decoupled from the [CoordinatorState]
    /// i.e. updates are not reflected
    // pub(crate) fn get_serializable(&self) -> SerializableClusterHandle {
    //     let worker_states = self
    //         .workers
    //         .iter()
    //         .map(|(id, (state, _))| (id, state))
    //         .cloned()
    //         .collect();
    //     SerializableClusterHandle {
    //         worker_states,
    //         config_version: self.config_version,
    //         snapshot_version: self.snapshot_version,
    //     }
    // }

    /// Start execution graph build on all workers
    /// Completes when all workers have finished building
    pub async fn start_build(&self) -> () {
        let build_info = BuildInformation {
            worker_set: self.workers.keys().map(|x| *x).collect(),
            resume_snapshot: self.snapshot_version,
            config_version: self.config_version.unwrap_or_default()
        };
        let msg = StartBuild(build_info);
        let responses = self
            .workers
            .values()
            .filter(|x| x.0.phase == WorkerPhase::Unknown)
            .map(|(_, client)| client.send::<_, ()>(msg.clone()));
        join_all(responses).await;
    }

    /// Start job execution on all workers
    pub async fn start_execution(&self) -> () {
        let msg = StartExecution;
        let responses = self
            .workers
            .values()
            .filter(|x| x.0.phase == WorkerPhase::BuildComplete)
            .map(|(_, client)| client.send::<_, ()>(msg.clone()));
        join_all(responses).await;
    }

    /// check if all workers have completed execution
    pub async fn check_execution_complete(&self) -> bool {
        let msg = ExecutionComplete;
        let responses = self
            .workers
            .values()
            .map(|(_, client)| client.send(msg.clone()));
        join_all(responses).await.into_iter().all(|x| x)
    }

    pub async fn take_snapshot(&self, version: SnapshotVersion) {
        let msg = RuntimeMessage::Snapshot(version);
        let responses = self
            .workers
            .values()
            .map(|(_, client)| client.send::<_, bool>(msg.clone()));
        join_all(responses).await.into_iter();
    }

    pub async fn reconfigure<C>(
        &mut self,
        new_set: IndexSet<WorkerId>,
        comm: &C,
    ) -> Result<(), Box<dyn std::error::Error>>
    where
        C: WorkerCoordinatorComm,
    {
        for wid in new_set.iter() {
            if !self.workers.contains_key(wid) {
                self.add_worker(*wid, comm).await?
            }
        }
        self.start_build().await;
        self.start_execution().await;
        let next_version = self.config_version.map(|x| x + 1).unwrap_or_default();
        let msg = RuntimeMessage::Reconfigure((new_set.clone(), next_version));
        let responses = self
            .workers
            .iter()
            .map(|(wid, (_, client))| client.send::<_, bool>(msg.clone()));
        join_all(responses).await;
        self.workers.retain(|wid, _| new_set.contains(wid));
        self.config_version = Some(next_version);
        Ok(())
    }

    /// Add, build and start a new worker
    async fn add_worker<C>(&mut self, id: WorkerId, comm: &C) -> Result<(), Box<dyn std::error::Error>>
    where
        C: WorkerCoordinatorComm,
    {
        let client = CoordinatorClient::new(id, comm).await?;
        self.workers.insert(id, (WorkerState::default(), client));
        Ok(())
    }
}

/// A serializable version of the [CoordinatorState]
#[derive(Serialize, Deserialize)]
pub(crate) struct SerializableClusterHandle {
    pub(super) worker_states: IndexMap<WorkerId, WorkerState>,
    pub(super) config_version: Option<u64>,
    /// Last snapshot the coordinator has completed or None if not yet any
    pub snapshot_version: Option<SnapshotVersion>,
}

impl SerializableClusterHandle {
    fn new(scale: u64) -> Self {
        let worker_states = (0..scale).map(|id| (id, WorkerState::default())).collect();
        Self {
            worker_states,
            config_version: None,
            snapshot_version: None,
        }
    }

    /// Load state from its serializable representation
    pub(crate) async fn setup_communication<C>(self, comm: &C) -> Result<ClusterHandle, Box<dyn std::error::Error>>
    where
        C: WorkerCoordinatorComm,
    {
        let worker_states = IndexMap::from(self.worker_states);
        let workers = IndexMap::with_capacity(worker_states.len());
        let mut cluster = ClusterHandle {
            workers,
            config_version: self.config_version,
            snapshot_version: self.snapshot_version,
        };
        for (id, state) in worker_states.into_iter() {
            let client = CoordinatorClient::new(id, comm).await?;
            cluster.workers.insert(id, (state, client));
        }
        Ok(cluster)
    }
}

impl From<&ClusterHandle> for SerializableClusterHandle {
    fn from(value: &ClusterHandle) -> Self {
        let worker_states = value.workers.iter().map(|(wid, x)| (*wid, x.0.clone())).collect();
        Self { worker_states, config_version: value.config_version, snapshot_version: value.snapshot_version }
    }
}

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

pub(super) fn load_or_create_cluster_handle<P>(
    backend: P,
    default_scale: u64,
) -> SerializableClusterHandle
where
    P: PersistenceBackend,
{
    let persisted = backend.last_commited();
    match persisted {
        Some(version) => {
            let persistence_client = backend.for_version(COORDINATOR_ID, &version);
            persistence_client
                .load(&0)
                .map(deserialize_state)
                .unwrap_or_else(|| SerializableClusterHandle::new(default_scale))
        }
        None => SerializableClusterHandle::new(default_scale),
    }
}
