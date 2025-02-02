use std::{future::Future, hash::Hash, sync::Arc, time::Duration};

use async_trait::async_trait;
use futures::{future::join_all, stream::FuturesUnordered};
use indexmap::{IndexMap, IndexSet};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::sync::Mutex;
use thiserror::Error;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use crate::{
    coordinator::{
        failfast::FailFast,
        state::{CoordinatorState, SerializableCoordinatorState},
        types::BuildInformation,
        watchmap::ConditionIter,
    },
    runtime::CommunicationClient,
    snapshot::{
        deserialize_state, serialize_state, PersistenceBackend, PersistenceClient, SnapshotVersion,
    },
    types::WorkerId,
};

use crate::runtime::communication::{CommunicationBackendError, CoordinatorWorkerComm};

use super::{
    communication::{setup_comm, WorkerSender},
    state::{WorkerPhase, WorkerState},
    types::{CoordinationMessage, WorkerMessage},
    watchmap::WatchMap,
};

/// This way we do not need seperate IDs for worker and coordinator
const COORDINATOR_ID: WorkerId = WorkerId::MAX;

pub struct Coordinator {
    _rt: tokio::runtime::Runtime,
    _tasks: tokio::task::JoinHandle<()>,
    req_tx: flume::Sender<CoordinatorRequest>,
}

impl Coordinator {
    pub fn new<C: CoordinatorWorkerComm + Send + Sync + 'static, P: PersistenceBackend + Send>(
        default_scale: u64,
        snapshot_interval: Option<Duration>,
        persistence: P,
        communication: C,
    ) -> Result<Coordinator, CoordinatorCreationError> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_time()
            .build()?;

        // load last state or create it
        let persisted = persistence.last_commited();
        let (snap_version, state) = match persisted {
            Some(p) => {
                let client = persistence.for_version(COORDINATOR_ID, &p);
                (
                    Some(p),
                    client
                        .load(&0)
                        .map(deserialize_state::<SerializableCoordinatorState>)
                        .unwrap_or_default(),
                )
            }
            None => (None, SerializableCoordinatorState::default()),
        };

        // channel for making API requests to the coordinator
        let (req_tx, req_rx) = flume::bounded(16);
        // send reconfig req to get the default scale if we have no workers
        if state.active_workers.len() == 0 {
            rt.spawn(CoordinatorRequest::send(
                RequestOperation::Scale(default_scale),
                req_tx.clone(),
            ));
        }
        // subtasks
        let subtasks = FuturesUnordered::new();

        debug!("Spawning coordinator loop");
        subtasks.push(rt.spawn(coordinator_loop(
            snap_version,
            state,
            req_rx,
            communication,
            persistence,
        )));
        if let Some(s) = snapshot_interval {
            subtasks.push(rt.spawn(auto_snapshot(s, req_tx.clone())));
        }
        let tasks = rt.spawn(async move { subtasks.failfast().await.unwrap().unwrap() });

        Ok(Self {
            _rt: rt,
            _tasks: tasks,
            req_tx,
        })
    }
}

async fn auto_snapshot(
    snapshot_interval: Duration,
    req_tx: flume::Sender<CoordinatorRequest>,
) -> () {
    loop {
        tokio::time::sleep(snapshot_interval).await;
        match CoordinatorRequest::send(RequestOperation::Snapshot, req_tx.clone()).await {
            Ok(_) => info!("Completed automatic snapshot"),
            Err(CoordinatorError::NotRunning) => {
                error!("Snapshot skipped, coordinator not running. No further snapshots will be attempted");
                return;
            }
            Err(CoordinatorError::ConcurrentOperation(e)) => {
                warn!("Snapshot skipped due to concurrent operation: {e:?}")
            }
        }
    }
}

impl Coordinator {
    pub async fn rescale(&self, desired: u64) -> Result<(), CoordinatorError> {
        CoordinatorRequest::send(RequestOperation::Scale(desired), self.req_tx.clone()).await
    }
}

#[derive(Debug, Error)]
pub enum CoordinatorCreationError {
    #[error(transparent)]
    CommunicationError(#[from] CommunicationBackendError),
    #[error("Error creating Tokio runtime: {0:?}")]
    RuntimeError(#[from] std::io::Error),
}

#[derive(Debug, Clone, Copy)]
pub enum CoordinatorStatus {
    Idle,
    Starting,
    Scaling,
    Snapshotting,
    Suspending,
}
impl From<RequestOperation> for CoordinatorStatus {
    fn from(value: RequestOperation) -> Self {
        match value {
            RequestOperation::Snapshot => Self::Snapshotting,
            RequestOperation::Scale(_) => Self::Scaling,
            RequestOperation::Suspend => Self::Suspending,
        }
    }
}

#[derive(Debug)]
struct CoordinatorRequest {
    callback: tokio::sync::oneshot::Sender<Result<(), CoordinatorError>>,
    request: RequestOperation,
}
impl CoordinatorRequest {
    async fn send(
        request: RequestOperation,
        channnel: flume::Sender<CoordinatorRequest>,
    ) -> Result<(), CoordinatorError> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let req = CoordinatorRequest {
            callback: tx,
            request,
        };
        channnel
            .send_async(req)
            .await
            .map_err(|_| CoordinatorError::NotRunning)?;
        rx.await.map_err(|_| CoordinatorError::NotRunning)?
    }
}

#[derive(Debug, Clone, Copy)]
enum RequestOperation {
    Snapshot,
    Scale(u64),
    Suspend,
}

#[derive(Debug, Error)]
pub enum CoordinatorError {
    #[error("Another operation is in progress: {0:?}")]
    ConcurrentOperation(#[from] SetStatusError),
    #[error("Coordinator is not running")]
    NotRunning,
}

async fn coordinator_loop<C: Send + CoordinatorWorkerComm, P: Send + PersistenceBackend>(
    mut snapshot_version: Option<SnapshotVersion>,
    state: SerializableCoordinatorState,
    requests: flume::Receiver<CoordinatorRequest>,
    communication_backend: C,
    persistence_backend: P,
) -> () {
    let mut state = CoordinatorState::from_serialized(state, &communication_backend).await;

    start_build_all(&state, snapshot_version).await;
    start_execution_all(&state).await;

    let execution_complete = tokio::spawn(
        state
            .worker_states
            .notify(AllPhaseCondition(WorkerPhase::Completed))
            .await,
    );

    let coordinator_status = Arc::new(Mutex::new(CoordinatorStatus::Idle));
    loop {
        if execution_complete.is_finished() {
            execution_complete.await.unwrap().unwrap();
            info!("Execution completed on all workers");
            return;
        }

        if let Ok(Ok(req)) =
            tokio::time::timeout(Duration::from_millis(100), requests.recv_async()).await
        {
            debug!("Handling request: {req:?}");
            let _guard = match coordinator_status
                .set_status(CoordinatorStatus::from(req.request))
                .await
            {
                Ok(guard) => guard,
                Err(e) => {
                    let _ = req.callback.send(Err(CoordinatorError::from(e)));
                    continue;
                }
            };
            match req.request {
                RequestOperation::Snapshot => {
                    let next_version = snapshot_version.map(|x| x + 1).unwrap_or(0);
                    perform_snapshot(&state, next_version).await;

                    let serialized_state = serialize_state(&state.get_serializable().await);
                    persistence_backend
                        .for_version(COORDINATOR_ID, &next_version)
                        .persist(&serialized_state, &0);
                    persistence_backend.commit_version(&next_version);
                    snapshot_version = Some(next_version);
                }
                RequestOperation::Scale(desired) => {
                    let diff = desired.abs_diff(state.active_workers.len() as u64);
                    if diff != 0 {
                        let new_config: IndexSet<WorkerId> = (0..desired).collect();
                        perform_reconfig(&mut state, new_config, &communication_backend).await;
                    }
                }
                RequestOperation::Suspend => {
                    perform_suspend(&mut state).await;
                }
            };
            // ignore since it is fine for us if the requester did not wait for
            // a response
            let _ = req.callback.send(Ok(()));
            if state.active_workers.len() == 0 {
                info!("Exiting coordinator: No workers are running");
                return;
            }
        }
    }
}

// async fn start_execution(state: &CoordinatorState) {
//     for c in state.active_workers.values() {
//         c.send(CoordinationMessage::StartExecution);
//     }
//     let worker_set: IndexSet<WorkerId> = state.active_workers.keys().cloned().collect();
//     info!(
//         "Awaiting execution start from {} worker(s)",
//         worker_set.len()
//     );
//     state
//         .worker_states
//         .notify(AllPhaseCondition(WorkerPhase::Running, Some(worker_set)))
//         .await
//         .await
//         .unwrap();
//     info!("Execution started on all workers");
// }

/// Start the build on all workers
async fn start_build_all(state: &CoordinatorState, snapshot_version: Option<SnapshotVersion>) {
    let futs = state
        .active_workers
        .keys()
        .map(|wid| start_build(*wid, state, snapshot_version.clone()));
    join_all(futs).await;
}

async fn start_execution_all(state: &CoordinatorState) {
    let futs = state
        .active_workers
        .keys()
        .map(|wid| start_execution(*wid, state));
    join_all(futs).await;
}

// async fn start_build(
//     state: &CoordinatorState,
//     worker_set: IndexSet<WorkerId>,
//     snapshot_version: Option<SnapshotVersion>,
// ) {
//     let all_workers = state.active_workers.keys().chain(worker_set.iter()).cloned().collect();
//     let build_info = BuildInformation {
//         worker_set: all_workers,
//         resume_snapshot: snapshot_version,
//     };
//     let to_build = state.active_workers.iter().filter_map(|(k, v)| worker_set.contains(k).then_some(v));

//     for c in to_build {
//         c.send(CoordinationMessage::StartBuild(build_info.clone()));
//     }
//     info!("Awaiting build from {} worker(s)", worker_set.len());
//     state
//         .worker_states
//         .notify(PhaseCondition(WorkerPhase::BuildComplete, Some(worker_set)))
//         .await
//         .await
//         .unwrap();
//     info!("Build completed");
// }

/// Start execution graph build on a single worker
async fn start_build(
    start_on: WorkerId,
    state: &CoordinatorState,
    snapshot_version: Option<SnapshotVersion>,
) {
    debug_assert!(state.active_workers.contains_key(&start_on));

    let build_info = BuildInformation {
        worker_set: state.active_workers.keys().cloned().collect(),
        resume_snapshot: snapshot_version,
    };
    state
        .active_workers
        .get(&start_on)
        .unwrap()
        .send(CoordinationMessage::StartBuild(build_info.clone()));
    info!("Awaiting build from worker {}", start_on);
    state
        .worker_states
        .notify(PhaseCondition(start_on, WorkerPhase::BuildComplete))
        .await
        .await
        .unwrap();
    info!("Build completed on {start_on}");
}

/// Start execution on a single worker
async fn start_execution(start_on: WorkerId, state: &CoordinatorState) {
    debug_assert!(state.active_workers.contains_key(&start_on));
    state
        .active_workers
        .get(&start_on)
        .unwrap()
        .send(CoordinationMessage::StartExecution);
    info!("Awaiting execution start from worker {start_on}");
    state
        .worker_states
        .notify(PhaseCondition(start_on, WorkerPhase::Running))
        .await
        .await
        .unwrap();
    info!("Execution started on {start_on}");
}

async fn perform_snapshot(state: &CoordinatorState, snapshot_version: SnapshotVersion) {
    for c in state.active_workers.values() {
        c.send(CoordinationMessage::Snapshot(snapshot_version));
    }
    state
        .worker_states
        .notify(AllPhaseCondition(WorkerPhase::Snapshotting))
        .await
        .await
        .unwrap();
    state
        .worker_states
        .notify(AllPhaseCondition(WorkerPhase::Running))
        .await
        .await
        .unwrap();
}

async fn perform_suspend(state: &mut CoordinatorState) {
    for c in state.active_workers.values() {
        c.send(CoordinationMessage::Suspend);
    }
    state
        .worker_states
        .notify(AllPhaseCondition(WorkerPhase::Suspended))
        .await
        .await
        .unwrap();
    for wid in state.active_workers.drain(..) {
        state.worker_states.remove(&wid.0).await;
    }
}

async fn perform_reconfig<C>(
    state: &mut CoordinatorState,
    new_config: IndexSet<WorkerId>,
    backend: &C,
) -> ()
where
    C: CoordinatorWorkerComm,
{
    let new_workers = new_config
        .iter()
        .filter(|x| !state.active_workers.contains_key(*x))
        .cloned()
        .collect();
    let (new_senders, _new_receivers) =
        setup_comm(backend, &new_workers, &state.worker_states).await;

    for nw in new_workers.iter() {
        state.worker_states.insert(*nw, WorkerState::default());
    }
    merge_maps(&mut state.active_workers, new_senders);

    join_all(new_workers.iter().map(|wid| start_build(*wid, state, None))).await;
    join_all(new_workers.iter().map(|wid| start_execution(*wid, state))).await;

    let next_version = state.config_version.map(|x| x + 1).unwrap_or_default();
    debug!("Reconfiguring with version {next_version}");

    for c in state.active_workers.values() {
        c.send(CoordinationMessage::Reconfigure((
            new_config.clone(),
            next_version,
        )));
    }
    state
        .worker_states
        .notify(AllPhaseCondition(WorkerPhase::Reconfiguring))
        .await
        .await
        .unwrap();
    state
        .worker_states
        .notify(AllPhaseCondition(WorkerPhase::Running))
        .await
        .await
        .unwrap();

    let to_remove = state
        .active_workers
        .keys()
        .filter(|wid| !new_config.contains(*wid))
        .cloned()
        .collect_vec();
    for wid in to_remove.into_iter() {
        state.worker_states.remove(&wid);
        state.active_workers.swap_remove(&wid);
    }
    state.config_version = Some(next_version);
}

struct AllPhaseCondition(WorkerPhase);

impl super::watchmap::Condition<WorkerId, WorkerState> for AllPhaseCondition {
    fn evaluate(&self, items: &mut ConditionIter<WorkerId, WorkerState>) -> bool {
        items.all(|(_, v)| v.phase == self.0)
    }
}

struct PhaseCondition(WorkerId, WorkerPhase);

impl super::watchmap::Condition<WorkerId, WorkerState> for PhaseCondition {
    fn evaluate(&self, items: &mut ConditionIter<WorkerId, WorkerState>) -> bool {
        items
            // use any to not return true on empty map
            .any(|(k, v)| *k == self.0 && v.phase == self.1)
    }
}

struct StatusGuard {
    owner: Arc<Mutex<CoordinatorStatus>>,
}

impl Drop for StatusGuard {
    fn drop(&mut self) {
        let mut owner = self.owner.lock().unwrap();
        let mut idle = CoordinatorStatus::Idle;
        std::mem::swap(&mut *owner, &mut idle);
    }
}

#[derive(Debug, Error)]
pub enum SetStatusError {
    #[error("Status already set by concurrent operation: {0:?}")]
    ConcurrentOperation(CoordinatorStatus),
}

#[async_trait]
trait SetStatus {
    async fn set_status(&self, status: CoordinatorStatus) -> Result<StatusGuard, SetStatusError>;
}
#[async_trait]
impl SetStatus for Arc<Mutex<CoordinatorStatus>> {
    async fn set_status(&self, status: CoordinatorStatus) -> Result<StatusGuard, SetStatusError> {
        let mut lock_guard = self.lock().unwrap();
        if let CoordinatorStatus::Idle = *lock_guard {
            *lock_guard = status;
            Ok(StatusGuard {
                owner: Arc::clone(&self),
            })
        } else {
            Err(SetStatusError::ConcurrentOperation(lock_guard.clone()))
        }
    }
}

// merge b into a
fn merge_maps<K, V>(a: &mut IndexMap<K, V>, b: IndexMap<K, V>)
where
    K: Hash + Eq,
{
    for (k, v) in b.into_iter() {
        a.insert(k, v);
    }
}
