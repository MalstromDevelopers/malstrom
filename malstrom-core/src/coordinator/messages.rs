use std::time::Duration;

use indexmap::{IndexMap, IndexSet};
use itertools::Itertools as _;
use serde::{Deserialize, Serialize};
use tracing::{debug, info};

use crate::{
    runtime::{
        communication::{
            broadcast, CommunicationBackendError, CoordinatorWorkerComm, Distributable,
            WorkerCoordinatorComm,
        },
        CommunicationClient,
    },
    types::WorkerId,
};

/// The Coordinator sends this to the Worker on startup
/// to give the worker the info it needs for building
#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub(crate) struct BuildInformation {
    /// Amount of workers in cluster
    pub(crate) scale: u64,
    /// snapshot which the workers shall load
    /// or none if starting fresh
    pub(crate) resume_snapshot: u64,
}

/// The worker sends this message once it is ready for
/// execution (after build)
#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub(crate) struct ExecutionReady;

/// The coordinator sends this message to trigger execution on all workers
#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub(crate) struct ExecutionStart;

/// The workers reply with this message once they have started execution
/// The coordinator sends this message to trigger execution on all workers
#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub(crate) struct ExecutionStartConfirm;

/// A message which can not be sent, because it is not expected
#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub(crate) enum NoMessage {}

/// Sent by worker to indicate snapshot completion
#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub(crate) struct SnapshotComplete;

/// Sent by worker to indicate rescale completion
#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub(crate) struct RescaleComplete;

/// Last message sent by worker before suspending
#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub(crate) struct SuspendComplete;

/// Last message sent by worker when it has finished execution
#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub(crate) struct ExecutionComplete;

/// Sent by coordinator to confirm to worker that it may shut down
#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub(crate) struct ConfirmExecutionComplete;

/// These can be sent at any time after execution start by the coordinator
#[derive(Clone, Serialize, Deserialize)]
pub(crate) enum CoordinationMessage {
    Snapshot(u64),
    ScaleAdd(IndexSet<WorkerId>),
    ScaleRemove(IndexSet<WorkerId>),
    Suspend,
}

/// The coordinator holds these clients to communicate with the workers
pub(super) struct CoordinatorClients<C, TSend, TRecv> {
    inner: IndexMap<WorkerId, CommunicationClient<TSend, TRecv>>,
    backend: C,
}
impl<C, TSend, TRecv> CoordinatorClients<C, TSend, TRecv> {
    pub fn scale(&self) -> u64 {
        self.inner.len() as u64
    }
}

impl<C> CoordinatorClients<C, NoMessage, NoMessage>
where
    C: CoordinatorWorkerComm,
{
    pub fn new(
        scale: u64,
        backend: C,
    ) -> Result<CoordinatorClients<C, BuildInformation, ExecutionReady>, CommunicationBackendError>
    {
        Self::new_with_ids((0..scale).collect(), backend)
    }

    pub fn new_with_ids(
        worker_ids: IndexSet<WorkerId>,
        backend: C,
    ) -> Result<CoordinatorClients<C, BuildInformation, ExecutionReady>, CommunicationBackendError>
    {
        let mut clients = IndexMap::with_capacity(worker_ids.len());
        for wid in worker_ids.into_iter() {
            let client =
                CommunicationClient::<BuildInformation, ExecutionReady>::coordinator_to_worker(
                    wid, &backend,
                )?;
            clients.insert(wid, client);
        }
        Ok(CoordinatorClients {
            inner: clients,
            backend,
        })
    }
}

impl<C> CoordinatorClients<C, BuildInformation, ExecutionReady>
where
    C: CoordinatorWorkerComm,
{
    pub async fn start_build(
        self,
        scale: u64,
        resume_snapshot: u64,
    ) -> CoordinatorClients<C, ExecutionStart, ExecutionStartConfirm> {
        broadcast(
            self.inner.values(),
            BuildInformation {
                scale,
                resume_snapshot,
            },
        );
        debug!("build info sent, awaiting replies");
        let _ = wait_for_all_reply(&self.inner).await;
        CoordinatorClients {
            inner: transform_clients(self.inner),
            backend: self.backend,
        }
    }
}

impl<C> CoordinatorClients<C, ExecutionStart, ExecutionStartConfirm>
where
    C: CoordinatorWorkerComm,
{
    pub async fn start_execution(
        self,
    ) -> CoordinatorClients<C, CoordinationMessage, ExecutionComplete> {
        broadcast(self.inner.values(), ExecutionStart);
        let confirm: Clients<NoMessage, ExecutionStartConfirm> = transform_clients(self.inner);
        let _ = wait_for_all_reply(&confirm).await;
        CoordinatorClients {
            inner: transform_clients(confirm),
            backend: self.backend,
        }
    }
}

impl<C> CoordinatorClients<C, CoordinationMessage, ExecutionComplete>
where
    C: CoordinatorWorkerComm,
{
    pub async fn snapshot(
        self,
        snapshot_version: u64,
    ) -> CoordinatorClients<C, CoordinationMessage, ExecutionComplete> {
        broadcast(
            self.inner.values(),
            CoordinationMessage::Snapshot(snapshot_version),
        );
        let _ = wait_for_all_reply(&self.inner).await;
        CoordinatorClients {
            inner: transform_clients(self.inner),
            backend: self.backend,
        }
    }

    pub async fn scale_up(
        mut self,
        to_add: u64,
    ) -> CoordinatorClients<C, CoordinationMessage, ExecutionComplete> {
        let current = self.inner.len() as u64;
        if to_add == 0 {
            return self;
        }
        let desired = current + to_add;

        let new_clients: CoordinatorClients<C, BuildInformation, ExecutionReady> =
            CoordinatorClients::new_with_ids((current..desired).collect(), self.backend).unwrap();
        debug!("Sending build and exec start to upscaled workers");
        let new_scale = (new_clients.inner.len() + self.inner.len()) as u64;
        // TODO: 0 is not right, they should not load any snapshot
        let new_clients = new_clients.start_build(new_scale, 0).await.start_execution().await;
        debug!("Execution started on new workers");
        self.inner = self
            .inner
            .into_iter()
            .chain(new_clients.inner.into_iter())
            .collect();

        let msg = CoordinationMessage::ScaleAdd((current..desired).collect::<IndexSet<u64>>());
        broadcast(self.inner.values(), msg);
        debug!("Rescale command sent");
        let _ = wait_for_all_reply(&self.inner).await;
        CoordinatorClients {
            inner: transform_clients(self.inner),
            backend: new_clients.backend,
        }
    }

    pub async fn scale_down(
        mut self,
        to_remove: u64,
    ) -> CoordinatorClients<C, CoordinationMessage, ExecutionComplete> {
        let current = self.inner.len() as u64;
        if to_remove == 0 {
            return self;
        }
        let desired = current.checked_sub(to_remove).unwrap();

        let msg = CoordinationMessage::ScaleRemove((desired..current).collect::<IndexSet<u64>>());
        broadcast(self.inner.values(), msg);
        let _ = wait_for_all_reply(&self.inner).await;

        for wid in desired..current {
            let _ = self.inner.shift_remove(&wid);
        }
        CoordinatorClients {
            inner: transform_clients(self.inner),
            backend: self.backend,
        }
    }

    /// check if any worker completed execution and remove that worker
    pub fn check_execution_complete(self) -> Self {
        // TODO: I don't like this, but a bad implementation is better than no implementation
        let inner: IndexMap<WorkerId, CommunicationClient<CoordinationMessage, ExecutionComplete>> =
            self.inner
                .into_iter()
                .filter_map(|(wid, client)| {
                    if let Some(_) = client.recv() {
                        info!(worker_id = wid, "Coordinator confirm completed execution");
                        let client = client.transform::<ConfirmExecutionComplete, NoMessage>();
                        client.send(ConfirmExecutionComplete);
                        None
                    } else {
                        Some((wid, client))
                    }
                })
                .collect();
        Self {
            inner: inner,
            ..self
        }
    }
}

type Clients<TSend, TRecv> = IndexMap<WorkerId, CommunicationClient<TSend, TRecv>>;
#[inline(always)]
fn transform_clients<TSend, TRecv, TSendNew, TRecvNew>(
    clients: Clients<TSend, TRecv>,
) -> Clients<TSendNew, TRecvNew> {
    clients
        .into_iter()
        .map(|(wid, client)| (wid, client.transform()))
        .collect()
}

/// Not elegant, but works
async fn wait_for_all_reply<TSend, TRecv: Distributable>(
    clients: &IndexMap<WorkerId, CommunicationClient<TSend, TRecv>>,
) -> IndexMap<WorkerId, TRecv> {
    let mut awaiting_reply = clients.iter().collect_vec();
    let mut replies = IndexMap::with_capacity(clients.len());
    loop {
        awaiting_reply.retain(|(wid, client)| {
            if let Some(resp) = client.recv() {
                replies.insert(**wid, resp);
                false
            } else {
                true
            }
        });
        if awaiting_reply.len() == 0 {
            return replies;
        }
        tokio::task::yield_now().await
    }
}

async fn wait_for_message<TSend, TRecv: Distributable>(
    client: &CommunicationClient<TSend, TRecv>,
) -> TRecv {
    loop {
        if let Some(msg) = client.recv() {
            return msg;
        }
        tokio::task::yield_now().await
    }
}

pub(crate) struct WorkerClient<TSend, TRecv> {
    inner: CommunicationClient<TSend, TRecv>,
}
impl<TSend, TRecv> WorkerClient<TSend, TRecv> {
    /// change generics
    fn transform<TSendNew, TRecvNew>(self) -> WorkerClient<TSendNew, TRecvNew> {
        WorkerClient {
            inner: self.inner.transform(),
        }
    }
}

impl WorkerClient<NoMessage, BuildInformation> {
    pub fn new<C: WorkerCoordinatorComm>(backend: &C) -> Self {
        Self {
            inner: CommunicationClient::worker_to_coordinator(backend).unwrap(),
        }
    }

    pub async fn get_build_info(
        self,
    ) -> (
        BuildInformation,
        WorkerClient<ExecutionReady, ExecutionStart>,
    ) {
        let build_info = wait_for_message(&self.inner).await;
        (build_info, self.transform())
    }
}

impl WorkerClient<ExecutionReady, ExecutionStart> {
    /// Tell the coordinator we are ready to start execution and await confirmation
    pub async fn await_start(self) -> WorkerClient<ExecutionComplete, CoordinationMessage> {
        self.inner.send(ExecutionReady);
        let wait_for_start = self.transform();
        wait_for_message::<NoMessage, ExecutionStart>(&wait_for_start.inner).await;
        let confirm = wait_for_start.transform::<ExecutionStartConfirm, NoMessage>();
        confirm.inner.send(ExecutionStartConfirm);
        confirm.transform()
    }
}

pub enum ActionWorkerClient {
    Idle(WorkerClient<ExecutionComplete, CoordinationMessage>),
    Snapshot(WorkerClient<SnapshotComplete, NoMessage>, u64),
    ScaleAdd(WorkerClient<RescaleComplete, NoMessage>, IndexSet<u64>),
    ScaleRemove(WorkerClient<RescaleComplete, NoMessage>, IndexSet<u64>),
    Suspend(WorkerClient<SuspendComplete, NoMessage>),
}

impl WorkerClient<ExecutionComplete, CoordinationMessage> {
    pub fn recv(self) -> ActionWorkerClient {
        match self.inner.recv() {
            None => ActionWorkerClient::Idle(self),
            Some(CoordinationMessage::Snapshot(version)) => {
                ActionWorkerClient::Snapshot(self.transform(), version)
            }
            Some(CoordinationMessage::ScaleAdd(to_add)) => {
                ActionWorkerClient::ScaleAdd(self.transform(), to_add)
            }
            Some(CoordinationMessage::ScaleRemove(to_remove)) => {
                ActionWorkerClient::ScaleRemove(self.transform(), to_remove)
            }
            Some(CoordinationMessage::Suspend) => ActionWorkerClient::Suspend(self.transform()),
        }
    }
}

impl WorkerClient<SnapshotComplete, NoMessage> {
    /// Tell the coordinator we are ready to start execution and await confirmation
    pub fn complete_snapshot(self) -> WorkerClient<NoMessage, CoordinationMessage> {
        self.inner.send(SnapshotComplete);
        self.transform()
    }
}

impl WorkerClient<RescaleComplete, NoMessage> {
    /// Notify the coordinator that rescaling (add) complete and await confirmation
    pub fn complete_scale_add(self) -> WorkerClient<ExecutionComplete, CoordinationMessage> {
        self.inner.send(RescaleComplete);
        self.transform()
    }

    /// Notify the coordinator that rescaling (remove) complete and await confirmation
    pub fn complete_scale_remove(self) -> WorkerClient<ExecutionComplete, CoordinationMessage> {
        self.inner.send(RescaleComplete);
        self.transform()
    }
}

impl WorkerClient<SuspendComplete, NoMessage> {
    /// Notify the coordinator that suspension complete and await confirmation
    pub fn complete_suspend(self) -> WorkerClient<ExecutionComplete, CoordinationMessage> {
        self.inner.send(SuspendComplete);
        self.transform()
    }
}

impl WorkerClient<ExecutionComplete, CoordinationMessage> {
    /// Notify the coordinator that suspension complete and await confirmation
    pub fn signal_execution_complete(self) -> WorkerClient<NoMessage, ConfirmExecutionComplete> {
        self.inner.send(ExecutionComplete);
        self.transform()
    }
}

impl WorkerClient<NoMessage, ConfirmExecutionComplete> {
    /// Notify the coordinator that suspension complete and await confirmation
    pub async fn await_execution_complete(self) -> () {
        loop {
            if let Some(_) = self.inner.recv_async().await {
                return;
            }
        }
    }
}
