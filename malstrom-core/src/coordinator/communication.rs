//! Communication from Coordinator to workers and vice-versa
use super::{
    state::{WorkerPhase, WorkerState},
    types::{CoordinationMessage, WorkerMessage},
    watchmap::WatchMap,
};
use crate::{
    runtime::{
        communication::{CommunicationBackendError, CoordinatorWorkerComm},
        CommunicationClient,
    },
    types::WorkerId,
};
use indexmap::{IndexMap, IndexSet};
use std::sync::Arc;
use thiserror::Error;
use tokio::task::JoinHandle;
use tracing::debug;

/// Receiver for receiving messages from a specific worker
pub(super) struct WorkerReceiver {
    worker_id: WorkerId,
    inner: Arc<CommunicationClient<CoordinationMessage, WorkerMessage>>,
}
impl WorkerReceiver {
    /// Asynchronously receive a message from the Worker. Finishes once a message has been received.
    async fn recv_async(&self) -> WorkerMessage {
        self.inner.recv_async().await
    }
}

/// Sender for sending messages to a specific worker via the runtime's communication
#[derive(Clone)]
pub(super) struct WorkerSender {
    inner: Arc<CommunicationClient<CoordinationMessage, WorkerMessage>>,
}
impl WorkerSender {
    /// Send a message to the worker
    pub(super) fn send(&self, msg: CoordinationMessage) {
        self.inner.send(msg);
    }
}

/// Thread for handling the incoming communication from a worker
pub(super) async fn worker_comm_inbound(
    states: WatchMap<WorkerId, WorkerState>,
    client: WorkerReceiver,
) {
    let set_phase = |phase| {
        states.apply_or_default(client.worker_id, |state| {
            state.phase = phase;
        })
    };
    loop {
        let msg = client.recv_async().await;
        debug!("Received message from worker {} {msg:?}", client.worker_id);
        match msg {
            WorkerMessage::BuildComplete => set_phase(WorkerPhase::BuildComplete).await,
            WorkerMessage::ExecutionStarted => set_phase(WorkerPhase::Running).await,
            WorkerMessage::SnapshotStarted => set_phase(WorkerPhase::Snapshotting).await,
            WorkerMessage::SnapshotComplete(version) => {
                states
                    .apply_or_default(client.worker_id, |state| {
                        state.phase = WorkerPhase::Running;
                        state.snapshot_version = Some(version);
                    })
                    .await;
            }
            WorkerMessage::ReconfigurationStarted => set_phase(WorkerPhase::Reconfiguring).await,
            WorkerMessage::ReconfigureComplete(_version) => {
                states
                    .apply_or_default(client.worker_id, |state| {
                        state.phase = WorkerPhase::Running;
                    })
                    .await;
            }
            WorkerMessage::ExecutionComplete => set_phase(WorkerPhase::Completed).await,
            WorkerMessage::SuspendComplete => set_phase(WorkerPhase::Suspended).await,
            // TODO: test this
            WorkerMessage::Removed => {
                states.remove(&client.worker_id).await;
                return;
            }
        }
    }
}

/// Set up communications with all workers.
/// The given global state will be kept up-to-date using the Messages exchanged with workers.
pub(super) async fn setup_comm<C>(
    comm: &C,
    worker_ids: &IndexSet<WorkerId>,
    global_state: &WatchMap<WorkerId, WorkerState>,
) -> Result<
    (
        IndexMap<WorkerId, WorkerSender>,
        IndexMap<WorkerId, JoinHandle<()>>,
    ),
    SetupCommunicationError,
>
where
    C: CoordinatorWorkerComm,
{
    let mut senders = IndexMap::with_capacity(worker_ids.len());
    let mut receiver_tasks = IndexMap::with_capacity(worker_ids.len());

    for wid in worker_ids.iter() {
        let client = Arc::new(CommunicationClient::coordinator_to_worker(*wid, comm)?);
        let sender = WorkerSender {
            inner: Arc::clone(&client),
        };
        let receiver = WorkerReceiver {
            worker_id: *wid,
            inner: Arc::clone(&client),
        };
        senders.insert(*wid, sender);

        global_state.insert(*wid, WorkerState::default()).await;
        receiver_tasks.insert(
            *wid,
            tokio::spawn(worker_comm_inbound(global_state.clone(), receiver)),
        );
    }
    Ok((senders, receiver_tasks))
}

#[derive(Debug, Error)]
pub enum SetupCommunicationError {
    #[error("Error from communication backend")]
    Backend(#[from] CommunicationBackendError),
}
