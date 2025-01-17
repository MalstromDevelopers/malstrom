use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use futures::{FutureExt, StreamExt};
use indexmap::{IndexMap, IndexSet};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{sync::Mutex, task::yield_now};
use tracing::{error, info, warn};

use crate::{
    snapshot::{
        deserialize_state, serialize_state,
        PersistenceBackend, PersistenceClient,
    },
    types::WorkerId,
};

use super::messages::*;
use crate::runtime::{
    communication::{CommunicationBackendError, CoordinatorWorkerComm, Distributable},
    CommunicationClient,
};

/// This way we do not need seperate IDs for worker and coordinator
const COORDINATOR_ID: WorkerId = WorkerId::MAX;

pub struct Coordinator {
    _rt: tokio::runtime::Runtime,
    _coordinator_loop: tokio::task::JoinHandle<()>,
    _auto_snapshot_loop: Option<tokio::task::JoinHandle<()>>,
}

impl Coordinator {
    pub fn new<C: CoordinatorWorkerComm + Send + 'static, P: PersistenceBackend + Send>(
        default_scale: u64,
        snapshot_interval: Option<Duration>,
        persistence: P,
        communication: C,
    ) -> Result<Coordinator, CoordinatorCreationError> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()?;

        let persistence_client = persistence.last_commited(COORDINATOR_ID);
        let state: CoordinatorState = persistence_client
            .load(&0)
            .map(deserialize_state)
            .unwrap_or(CoordinatorState {
                scale: default_scale,
            });

        let clients = CoordinatorClients::new(state.scale, communication)?;
        let (req_tx, req_rx) = flume::bounded(16);
        let status = Arc::new(Mutex::new(CoordinatorStatus::Idle));
        let coordinator_loop = rt.spawn(coordinator_loop(req_rx, status, clients, persistence));

        let auto_snap_tx = req_tx.clone();
        let auto_snapshot_loop = match snapshot_interval {
        Some(interval) => Some(rt.spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                match CoordinatorRequest::send(RequestOperation::Snapshot, &auto_snap_tx).await {
                    Ok(_) => info!("Completed automatic snapshot"),
                    Err(CoordinatorError::NotRunning) => {
                        error!("Snapshot skipped, coordinator not running. No further snapshots will be attempted")
                    },
                    Err(CoordinatorError::ConcurrentOperation(e)) => {
                        warn!("Snapshot skipped due to concurrent operation: {e:?}")
                    }
                }
            }
        })),
        None => None
        };

        Ok(Self {
            _rt: rt,
            _coordinator_loop: coordinator_loop,
            _auto_snapshot_loop: auto_snapshot_loop,
        })
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

#[derive(Clone, Serialize, Deserialize)]
pub(crate) enum CoordinationMessage {
    Snapshot(u64),
    ScaleAdd(IndexSet<WorkerId>),
    ScaleRemove(IndexSet<WorkerId>),
    Suspend,
}

struct CoordinatorRequest {
    callback: tokio::sync::oneshot::Sender<Result<(), CoordinatorError>>,
    request: RequestOperation,
}
impl CoordinatorRequest {
    async fn send(
        request: RequestOperation,
        channnel: &flume::Sender<CoordinatorRequest>,
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

#[derive(Serialize, Deserialize)]
struct CoordinatorState {
    scale: u64,
}

async fn coordinator_loop<C: Send + CoordinatorWorkerComm, P: Send + PersistenceBackend>(
    requests: flume::Receiver<CoordinatorRequest>,
    status: Arc<Mutex<CoordinatorStatus>>,
    clients: CoordinatorClients<C, BuildInformation, ExecutionReady>,
    persistence_backend: P,
) -> () {
    let mut last_snapshot = persistence_backend
        .last_commited(COORDINATOR_ID)
        .get_version();

    let clients = clients.start_build(last_snapshot).await;
    info!("Build completed on all workers");
    let mut clients = clients.start_execution().await;
    info!("Execution started on all workers");

    while let Ok(req) = requests.recv_async().await {
        let _guard = match status
            .set_status(CoordinatorStatus::from(req.request))
            .await
        {
            Ok(guard) => guard,
            Err(e) => {
                let _ = req.callback.send(Err(CoordinatorError::from(e)));
                continue;
            }
        };

        clients = match req.request {
            RequestOperation::Snapshot => {
                let next_version = last_snapshot + 1;
                let clients = clients.snapshot(next_version).await;
                let state = CoordinatorState {
                    scale: clients.scale(),
                };
                persistence_backend
                    .for_version(last_snapshot, &next_version)
                    .persist(&serialize_state(&state), &0);
                persistence_backend.commit_version(&next_version);
                last_snapshot = next_version;
                clients
            }
            RequestOperation::Scale(desired) => {
                let scale = clients.scale();
                let diff = desired.abs_diff(scale);
                if desired > scale {
                    clients.scale_up(diff).await
                } else {
                    clients.scale_down(diff).await
                }
            }
            RequestOperation::Suspend => {
                unimplemented!()
            }
        };
        // ignore since it is fine for us if the requester did not wait for
        // a response
        let _ = req.callback.send(Ok(()));
    }
}

struct StatusGuard {
    owner: Arc<Mutex<CoordinatorStatus>>,
}
impl Drop for StatusGuard {
    fn drop(&mut self) {
        let mut owner = self.owner.blocking_lock();
        let mut idle = CoordinatorStatus::Idle;
        std::mem::swap(&mut *owner, &mut idle);
    }
}

#[derive(Debug, Error)]
enum SetStatusError {
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
        let mut lock_guard = self.lock().await;
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
