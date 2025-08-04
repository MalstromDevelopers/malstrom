//! # Coordinator
//!
//! The coordinator handles operation in a distributed Malstrom job which require coordination
//! like
//! - rescaling
//! - snapshotting
//! - suspending and ending the job
//!
//! There is always one (and only one) Coordinator per job.
use super::{
    communication::{setup_comm, SetupCommunicationError},
    state::{FromSerializedError, WorkerPhase, WorkerState},
    types::CoordinationMessage,
};
use crate::runtime::communication::{CommunicationBackendError, CoordinatorWorkerComm};
use crate::{
    coordinator::{
        state::{CoordinatorState, SerializableCoordinatorState},
        types::BuildInformation,
        watchmap::ConditionIter,
    },
    snapshot::{
        deserialize_state, serialize_state, PersistenceBackend, PersistenceClient, SnapshotVersion,
    },
    types::WorkerId,
};
use async_trait::async_trait;
use futures::{future::join_all, TryFutureExt};
use indexmap::{IndexMap, IndexSet};
use itertools::Itertools;
use std::sync::Mutex;
use std::{hash::Hash, sync::Arc, time::Duration};
use thiserror::Error;
use tracing::{debug, error, info, warn};

/// This way we do not need seperate IDs for worker and coordinator
const COORDINATOR_ID: WorkerId = WorkerId::MAX;

/// Coordinator which controls a Malstrom job.
/// The coordinator coordinates job start/stop, rescaling and snapshotting.
pub struct Coordinator {
    // channel for making API requests to the coordinator
    req: (
        flume::Sender<CoordinatorRequest>,
        flume::Receiver<CoordinatorRequest>,
    ),
}

/// API handle for the [Coordinator]. Use this to send commands (like a rescale-command)
/// to the coordinator
pub struct CoordinatorApi {
    req_tx: flume::Sender<CoordinatorRequest>,
}

impl Coordinator {
    /// Create a new [Coordinator]. This should usually not be done directly as the coordinator
    /// will be created and owned by a Malstrom runtime.
    pub fn new() -> (Self, CoordinatorApi) {
        // channel for making API requests to the coordinator
        let req = flume::bounded(16);
        let api = CoordinatorApi {
            req_tx: req.0.clone(),
        };
        (Self { req }, api)
    }

    /// Start this Coordinator
    pub fn execute<
        C: CoordinatorWorkerComm + Send + Sync + 'static,
        P: PersistenceBackend + Send,
    >(
        self,
        default_scale: u64,
        snapshot_interval: Option<Duration>,
        persistence: P,
        communication: C,
    ) -> Result<(), CoordinatorExecutionError> {
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
        // send reconfig req to get the default scale if we have no workers
        if state.active_workers.is_empty() {
            rt.spawn(CoordinatorRequest::send(
                RequestOperation::Scale(default_scale),
                self.req.0.clone(),
            ));
        }
        let main_loop = rt.spawn(
            coordinator_loop(snap_version, state, self.req.1, communication, persistence)
                .map_err(CoordinatorExecutionError::from),
        );
        if let Some(s) = snapshot_interval {
            rt.spawn(auto_snapshot(s, self.req.0.clone()));
        }
        rt.block_on(main_loop)
            .map_err(CoordinatorExecutionError::CoordinatorLoopJoin)?
    }
}

/// Possible errors when executing a Coordinator
#[allow(missing_docs)]
#[derive(Debug, Error)]
pub enum CoordinatorExecutionError {
    #[error(transparent)]
    CommunicationError(#[from] CommunicationBackendError),
    #[error("Error creating Tokio runtime: {0:?}")]
    RuntimeError(#[from] std::io::Error),
    #[error("Error in coordinator loop")]
    CoordinatorLoop(#[from] CoordinatorLoopError),
    #[error("Error joining coordinator loop task")]
    CoordinatorLoopJoin(#[source] tokio::task::JoinError),
}

/// Thread for performing automatic interval snapshots
async fn auto_snapshot(snapshot_interval: Duration, req_tx: flume::Sender<CoordinatorRequest>) {
    loop {
        tokio::time::sleep(snapshot_interval).await;
        match CoordinatorRequest::send(RequestOperation::Snapshot, req_tx.clone()).await {
            Ok(_) => info!("Completed automatic snapshot"),
            Err(CoordinatorRequestError::NotRunning) => {
                error!("Snapshot skipped, coordinator not running. No further snapshots will be attempted");
                return;
            }
            Err(CoordinatorRequestError::ConcurrentOperation(e)) => {
                warn!("Snapshot skipped due to concurrent operation: {e:?}")
            }
        }
    }
}

impl CoordinatorApi {
    /// Rescale the Malstrom job to a desired parallelism.
    /// This will perform a zero-downtime rescaling and distribute all worker state accordingly.
    /// If the desired scale == current scale, this is a no-op.
    ///
    /// # Arguments
    /// - desired: Desired parallelism
    pub async fn rescale(&self, desired: u64) -> Result<(), CoordinatorRequestError> {
        CoordinatorRequest::send(RequestOperation::Scale(desired), self.req_tx.clone()).await
    }
}

/// Lifecycle statuses of the coordinator
#[derive(Debug, Clone, Copy)]
pub enum CoordinatorStatus {
    /// Coordinator is not currently performing any action
    Idle,
    /// Coordinator is currently starting the job
    Starting,
    /// Coordinator is currently rescaling the job
    Scaling,
    /// Coordinator is currently performing a global snapshot
    Snapshotting,
    /// Coordinator is currently suspending job execution
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

/// New request for what the coordinator should do
#[derive(Debug)]
struct CoordinatorRequest {
    /// Oneshot which completes as soon as the coordinator has fullfilled the request
    /// or encountered and error
    callback: tokio::sync::oneshot::Sender<Result<(), CoordinatorRequestError>>,
    /// Operation to perform
    request: RequestOperation,
}
impl CoordinatorRequest {
    /// Send a new request of the given operation to the coordinator. Future resolves as soon as
    /// the request has been completed or errored
    async fn send(
        request: RequestOperation,
        channnel: flume::Sender<CoordinatorRequest>,
    ) -> Result<(), CoordinatorRequestError> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let req = CoordinatorRequest {
            callback: tx,
            request,
        };
        channnel
            .send_async(req)
            .await
            .map_err(|_| CoordinatorRequestError::NotRunning)?;
        rx.await.map_err(|_| CoordinatorRequestError::NotRunning)?
    }
}

/// Request the Coordinator to do something
#[derive(Debug, Clone, Copy)]
enum RequestOperation {
    /// Request coordinator to perform a global state snapshot
    Snapshot,
    /// Request coordinator to rescale the job to this parallelism
    Scale(u64),
    /// UNIMPLEMENTED: Request Coordinator to suspend the execution
    #[allow(unused)] // TODO
    Suspend,
}

/// Possible errors returned by Coordinator when asked to perform requested Action
#[allow(missing_docs)]
#[derive(Debug, Error)]
pub enum CoordinatorRequestError {
    #[error("Another operation is in progress: {0:?}")]
    ConcurrentOperation(#[from] SetStatusError),
    #[error("Coordinator is not running")]
    NotRunning,
}

/// Create a new coordinator loop. This creates a coordinator and starts it.
/// The returned future resolves once the coordinator terminates
///
/// # Arguments
/// - snapshot_version: Snapshot version to start job from or `None` to do stateless start
/// - state: Coordinator state to start from
/// - requests: Channel to listen on for incoming requests like rescale, suspend etc.
/// - communication_backend: Backend to communicate with workers
/// - persistence_backend: Backend for storing coordinator state on snapshots
async fn coordinator_loop<C: Send + CoordinatorWorkerComm, P: Send + PersistenceBackend>(
    mut snapshot_version: Option<SnapshotVersion>,
    state: SerializableCoordinatorState,
    requests: flume::Receiver<CoordinatorRequest>,
    communication_backend: C,
    mut persistence_backend: P,
) -> Result<(), CoordinatorLoopError> {
    let mut state = CoordinatorState::from_serialized(state, &communication_backend).await?;
    // start job on all workers
    start_build_all(&state, snapshot_version).await;
    start_execution_all(&state).await;

    let completion_notification = state
        .worker_states
        .notify(AllPhaseCondition(WorkerPhase::Completed))
        .await;
    let execution_complete = tokio::spawn(async {
        completion_notification
            .await
            .expect("Notification should never be dropped")
    });

    let coordinator_status = Arc::new(Mutex::new(CoordinatorStatus::Idle));
    loop {
        if execution_complete.is_finished() {
            execution_complete.await?;
            info!("Execution completed on all workers");
            return Ok(());
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
                    let _ = req.callback.send(Err(CoordinatorRequestError::from(e)));
                    continue;
                }
            };
            match req.request {
                RequestOperation::Snapshot => {
                    let next_version = snapshot_version.map(|x| x + 1).unwrap_or(0);
                    perform_snapshot_all(&state, next_version).await;

                    let serialized_state = serialize_state(&state.get_serializable().await);
                    persistence_backend = tokio::runtime::Handle::current()
                        .spawn_blocking(move || {
                            persistence_backend
                                .for_version(COORDINATOR_ID, &next_version)
                                .persist(&serialized_state, &0);
                            persistence_backend.commit_version(&next_version);
                            persistence_backend
                        })
                        .await?;
                    snapshot_version = Some(next_version);
                }
                RequestOperation::Scale(desired) => {
                    let diff = desired.abs_diff(state.active_workers.len() as u64);
                    if diff != 0 {
                        let new_config: IndexSet<WorkerId> = (0..desired).collect();
                        info!("Starting rescale to {new_config:?}");
                        perform_reconfig_all(&mut state, new_config, &communication_backend)
                            .await?;
                    }
                }
                RequestOperation::Suspend => {
                    perform_suspend_all(&mut state).await;
                }
            };
            // ignore since it is fine for us if the requester did not wait for
            // a response
            let _ = req.callback.send(Ok(()));
            if state.active_workers.is_empty() {
                info!("Exiting coordinator: No workers are running");
                return Ok(());
            }
        }
    }
}

#[derive(Debug, Error)]
pub enum CoordinatorLoopError {
    #[error("Error creating coordinator state from serialized state")]
    FromSerialized(#[from] FromSerializedError),
    #[error("Error performing reconfiguration on all workers")]
    PerformReconfigAll(#[from] PerformReconfigAllError),
    #[error(transparent)]
    TokioJoin(#[from] tokio::task::JoinError),
}

/// Start the build on all workers
async fn start_build_all(state: &CoordinatorState, snapshot_version: Option<SnapshotVersion>) {
    let futs = state
        .active_workers
        .keys()
        .map(|wid| start_build(*wid, state, snapshot_version));
    join_all(futs).await;
}

/// Start job execution on all workers
async fn start_execution_all(state: &CoordinatorState) {
    let futs = state
        .active_workers
        .keys()
        .map(|wid| start_execution(*wid, state));
    join_all(futs).await;
}

/// Start execution graph build on a single worker
async fn start_build(
    start_on: WorkerId,
    state: &CoordinatorState,
    snapshot_version: Option<SnapshotVersion>,
) -> Result<(), StartBuildError> {
    debug_assert!(state.active_workers.contains_key(&start_on));

    let build_info = BuildInformation {
        worker_set: state.active_workers.keys().cloned().collect(),
        resume_snapshot: snapshot_version,
    };
    state
        .active_workers
        .get(&start_on)
        .ok_or(StartBuildError::UnknownWorker(start_on))?
        .send(CoordinationMessage::StartBuild(build_info.clone()));
    info!("Awaiting build from worker {}", start_on);
    state
        .worker_states
        .notify(PhaseCondition(start_on, WorkerPhase::BuildComplete))
        .await
        .await?;
    info!("Build completed on {start_on}");
    Ok(())
}

#[derive(Debug, Error)]
enum StartBuildError {
    #[error("Worker with ID {0} is not in set of known workers")]
    UnknownWorker(WorkerId),
    #[error("Error watching for status change on worker")]
    StatusChange(#[from] tokio::sync::oneshot::error::RecvError),
}

/// Start execution on a single worker
async fn start_execution(
    start_on: WorkerId,
    state: &CoordinatorState,
) -> Result<(), StartExecutionError> {
    debug_assert!(state.active_workers.contains_key(&start_on));
    state
        .active_workers
        .get(&start_on)
        .ok_or(StartExecutionError::UnknownWorker(start_on))?
        .send(CoordinationMessage::StartExecution);
    info!("Awaiting execution start from worker {start_on}");
    state
        .worker_states
        .notify(PhaseCondition(start_on, WorkerPhase::Running))
        .await
        .await?;
    info!("Execution started on {start_on}");
    Ok(())
}

#[derive(Debug, Error)]
enum StartExecutionError {
    #[error("Worker with ID {0} is not in set of known workers")]
    UnknownWorker(WorkerId),
    #[error("Error watching for status change on worker")]
    StatusChange(#[from] tokio::sync::oneshot::error::RecvError),
}

/// Perform a Snapshot on all workers. Resolves when snapshot completes
async fn perform_snapshot_all(state: &CoordinatorState, snapshot_version: SnapshotVersion) {
    for c in state.active_workers.values() {
        c.send(CoordinationMessage::Snapshot(snapshot_version));
    }
    state
        .worker_states
        .notify(AllPhaseCondition(WorkerPhase::Snapshotting))
        .await
        .await
        .expect("Notification should never be dropped");
    state
        .worker_states
        .notify(AllPhaseCondition(WorkerPhase::Running))
        .await
        .await
        .expect("Notification should never be dropped");
}

/// Suspend execution on all workers. Resolves once execution has been suspended.
async fn perform_suspend_all(state: &mut CoordinatorState) {
    for c in state.active_workers.values() {
        c.send(CoordinationMessage::Suspend);
    }
    state
        .worker_states
        .notify(AllPhaseCondition(WorkerPhase::Suspended))
        .await
        .await
        .expect("Notification should never be dropped");
    for wid in state.active_workers.drain(..) {
        state.worker_states.remove(&wid.0).await;
    }
}

/// Perform a job topology reconfiguration on all workers i.e. changing the set of workers in the
/// job usually due to rescaling.
async fn perform_reconfig_all<C>(
    state: &mut CoordinatorState,
    new_config: IndexSet<WorkerId>,
    backend: &C,
) -> Result<(), PerformReconfigAllError>
where
    C: CoordinatorWorkerComm,
{
    let new_workers = new_config
        .iter()
        .filter(|x| !state.active_workers.contains_key(*x))
        .cloned()
        .collect();
    let (new_senders, _new_receivers) =
        setup_comm(backend, &new_workers, &state.worker_states).await?;

    for nw in new_workers.iter() {
        state
            .worker_states
            .insert(*nw, WorkerState::default())
            .await;
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
        .expect("Notification should never be dropped");
    state
        .worker_states
        .notify(AllPhaseCondition(WorkerPhase::Running))
        .await
        .await
        .expect("Notification should never be dropped");

    let to_remove = state
        .active_workers
        .keys()
        .filter(|wid| !new_config.contains(*wid))
        .cloned()
        .collect_vec();
    for wid in to_remove.into_iter() {
        state.worker_states.remove(&wid).await;
        state.active_workers.swap_remove(&wid);
    }
    state.config_version = Some(next_version);
    Ok(())
}

#[derive(Debug, Error)]
pub enum PerformReconfigAllError {
    #[error("Error setting up communication")]
    SetupCommunication(#[from] SetupCommunicationError),
}

/// Helper type to await all workers reporting being in a certain state
struct AllPhaseCondition(WorkerPhase);

impl super::watchmap::Condition<WorkerId, WorkerState> for AllPhaseCondition {
    /// Check if all workers are in this condition's phase
    fn evaluate(&self, items: &mut ConditionIter<WorkerId, WorkerState>) -> bool {
        items.all(|(_, v)| v.phase == self.0)
    }
}

/// Helper type to await a single worker reporting being in a certain state
struct PhaseCondition(WorkerId, WorkerPhase);

impl super::watchmap::Condition<WorkerId, WorkerState> for PhaseCondition {
    /// Check if this condition's worker is in the target state
    fn evaluate(&self, items: &mut ConditionIter<WorkerId, WorkerState>) -> bool {
        items
            // use any to not return true on empty map
            .any(|(k, v)| *k == self.0 && v.phase == self.1)
    }
}

/// Guard type which helps ensure Coordinator only performs a single operation at a time
/// If the Coordinator is not idle, this guard can not be obtained.
struct CoordinatorStatusGuard {
    owner: Arc<Mutex<CoordinatorStatus>>,
}

impl Drop for CoordinatorStatusGuard {
    fn drop(&mut self) {
        match self.owner.lock() {
            Ok(mut owner) => {
                let mut idle = CoordinatorStatus::Idle;
                std::mem::swap(&mut *owner, &mut idle);
            }
            Err(_) => {
                // unwrapping in the drop is a no-no
                eprintln!("ERROR: Can not drop CoordinatorStatusGuard due to poisened Mutex");
            }
        }
    }
}

#[derive(Debug, Error)]
pub enum SetStatusError {
    #[error("Concurrent operation: {0:?}")]
    ConcurrentOperation(CoordinatorStatus),
}

#[async_trait]
trait SetStatus {
    /// Set the coordinator status. This fails if the coordinator is not currently idle
    async fn set_status(
        &self,
        status: CoordinatorStatus,
    ) -> Result<CoordinatorStatusGuard, SetStatusError>;
}
#[async_trait]
impl SetStatus for Arc<Mutex<CoordinatorStatus>> {
    async fn set_status(
        &self,
        status: CoordinatorStatus,
    ) -> Result<CoordinatorStatusGuard, SetStatusError> {
        #[allow(clippy::unwrap_used)]
        let mut lock_guard = self.lock().unwrap();
        if let CoordinatorStatus::Idle = *lock_guard {
            *lock_guard = status;
            Ok(CoordinatorStatusGuard {
                owner: Arc::clone(self),
            })
        } else {
            Err(SetStatusError::ConcurrentOperation(*lock_guard))
        }
    }
}

// merge b into a, overwriting values in a if the key is in both.
fn merge_maps<K, V>(a: &mut IndexMap<K, V>, b: IndexMap<K, V>)
where
    K: Hash + Eq,
{
    for (k, v) in b.into_iter() {
        a.insert(k, v);
    }
}
