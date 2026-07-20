//! # Coordinator
//!
//! The coordinator handles operation in a distributed Malstrom job which require coordination
//! like
//! - rescaling
//! - snapshotting
//! - suspending and ending the job
//!
//! There is always one (and only one) Coordinator per job.
use crate::{
    coordinator::{
        COORDINATOR_ID,
        api::{ApiRequest, ApiRequestOperation, CoordinatorApi},
        cluster::{SerializableClusterHandle, load_or_create_cluster_handle},
    },
    runtime::communication::WorkerCoordinatorComm,
};
use crate::{
    coordinator::{messages::BuildInformation, watchmap::ConditionIter},
    snapshot::{
        PersistenceBackend, PersistenceClient, SnapshotVersion, deserialize_state, serialize_state,
    },
    types::WorkerId,
};
use async_trait::async_trait;
use futures::{TryFutureExt, future::join_all};
use indexmap::{IndexMap, IndexSet};
use itertools::Itertools;
use std::sync::Mutex;
use std::{hash::Hash, sync::Arc, time::Duration};
use thiserror::Error;
use tracing::{debug, error, info, warn};

/// Coordinator which controls a Malstrom job.
/// The coordinator coordinates job start/stop, rescaling and snapshotting.
pub struct Coordinator {
    // channel for making API requests to the coordinator
    req: (flume::Sender<ApiRequest>, flume::Receiver<ApiRequest>),
}

impl Coordinator {
    /// Create a new [Coordinator]. This should usually not be done directly as the coordinator
    /// will be created and owned by a Malstrom runtime.
    pub fn new() -> (Self, CoordinatorApi) {
        // channel for making API requests to the coordinator
        let req = flume::bounded(16);
        let api = CoordinatorApi::new(req.0.clone());
        (Self { req }, api)
    }

    /// Start this Coordinator
    pub fn execute<
        C: WorkerCoordinatorComm + Send + Sync + 'static,
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

        let cluster = load_or_create_cluster_handle(persistence, default_scale);

        let main_loop = rt.spawn(
            coordinator_loop(cluster, self.req.1, communication, persistence)
                .map_err(CoordinatorExecutionError::from),
        );
        if let Some(s) = snapshot_interval {
            rt.spawn(super::snapshot::auto_snapshot(s, self.req.0.clone()));
        }
        rt.block_on(main_loop)
            .map_err(CoordinatorExecutionError::CoordinatorLoopJoin)?
    }
}

/// Possible errors when executing a Coordinator
#[allow(missing_docs)]
#[derive(Debug, Error)]
pub enum CoordinatorExecutionError {
    #[error("Error creating Tokio runtime: {0:?}")]
    RuntimeError(#[from] std::io::Error),
    #[error("Error joining coordinator loop task")]
    CoordinatorLoopJoin(#[source] tokio::task::JoinError),
    #[error("Error in coordinator")]
    CoordinatorTask(#[from] CoordinatorError),
}

/// Create a new coordinator loop. This creates a coordinator and starts it.
/// The returned future resolves once the coordinator terminates
async fn coordinator_loop<C, P>(
    state: SerializableClusterHandle,
    requests: flume::Receiver<ApiRequest>,
    communication_backend: C,
    mut persistence_backend: P,
) -> Result<(), CoordinatorError>
where
    C: Send + WorkerCoordinatorComm,
    P: Send + PersistenceBackend,
{
    let mut state = state
        .setup_communication(&communication_backend)
        .await
        .map_err(|_| CoordinatorError::Communication)?;
    // start job on all workers
    state.start_build().await;
    state.start_execution().await;

    loop {
        // either wake on API request or loop duration elapsed
        let api_request = {
            // no api handles, do not wait for API requests
            if requests.is_disconnected() {
                tokio::time::sleep(Duration::from_secs(5)).await;
                None
            } else {
                tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(5)) => {None}
                  x = requests.recv_async() => {
                      match x {
                          Ok(x) => Some(x),
                          Err(_) => continue // all senders dropped
                      }
                  }
                  }
            }
        };

        if state.check_execution_complete().await {
            info!("Execution completed on all workers");
            return Ok(());
        }

        if let Some(api_request) = api_request {
            let callback = api_request.callback;
            match api_request.request {
                ApiRequestOperation::Snapshot => {
                    let next_version = state.snapshot_version.map(|x| x + 1).unwrap_or(0);
                    state.take_snapshot(next_version).await;
                    state.snapshot_version = Some(next_version);
                    let serialized_state =
                        serialize_state(&SerializableClusterHandle::from(&state));
                    persistence_backend
                        .for_version(COORDINATOR_ID, &next_version)
                        .persist(&serialized_state, &0);
                    persistence_backend.commit_version(&next_version);
                }
                ApiRequestOperation::Scale(desired) => {
                    let diff = desired.abs_diff(state.workers.len() as u64);
                    if diff != 0 {
                        let worker_set: IndexSet<WorkerId> = (0..desired).collect();
                        info!("Starting rescale to {worker_set:?}");
                        state.reconfigure(worker_set, &communication_backend).await;
                        info!("Rescale complete");
                    }
                }
                ApiRequestOperation::Suspend => state.suspend().await,
            }
            // ignore since it is fine for us if the requester did not wait for
            // a response
            let _ = callback.send(Ok(()));
        }
    }
}

#[derive(Debug, Error)]
pub enum CoordinatorError {
    #[error("Error setting up communication to workers")]
    Communication,
    #[error(transparent)]
    TokioJoin(#[from] tokio::task::JoinError),
}
