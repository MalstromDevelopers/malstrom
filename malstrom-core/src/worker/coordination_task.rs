use std::{collections::HashMap, rc::Rc, sync::Mutex};

use indexmap::IndexSet;
use thiserror::Error;
use tokio::{runtime::LocalRuntime, sync::mpsc};
use tracing::info;

use crate::{
    channels::signal::SignalHandle,
    coordinator::types::{BuildInformation, CoordinationMessage, WorkerMessage},
    runtime::{
        CommunicationClient, OperatorOperatorComm, RuntimeFlavor,
        communication::WorkerCoordinatorComm,
    },
    snapshot::{NoPersistence, PersistenceBackend, PersistenceClient, SnapshotVersion},
    stream::{DirectLogic, Operator, WorkerBuildContext},
    types::WorkerId,
    worker::{InnerRuntimeBuilder, root_logic::RootLogic, sys_message::SysMessage},
};

/// Task for interacting with the central job coordinator
pub(super) struct CoordinationTask<P: PersistenceBackend> {
    worker_id: WorkerId,
    persistence_backend: P,
    sys_msg_sender: mpsc::Sender<SysMessage<P::Client>>,
    coordinator_comm: CommunicationClient<WorkerMessage, CoordinationMessage>,
}

impl<P> CoordinationTask<P>
where
    P: PersistenceBackend,
{
    pub(super) fn new(
        this_worker: WorkerId,
        persistence_backend: P,
        sys_msg_sender: mpsc::Sender<SysMessage<P::Client>>,
        coordinator_comm: CommunicationClient<WorkerMessage, CoordinationMessage>,
    ) -> Self {
        Self {
            worker_id: this_worker,
            persistence_backend,
            sys_msg_sender,
            coordinator_comm,
        }
    }

    pub(super) fn start(self, comm_rt: &tokio::runtime::Runtime) -> tokio::task::JoinHandle<()> {
        comm_rt.spawn(async move {
            loop {
                let msg = self.coordinator_comm.recv_async().await;
                match msg {
                    CoordinationMessage::StartBuild(_) => unreachable!(),
                    CoordinationMessage::StartExecution => unreachable!(),
                    CoordinationMessage::Snapshot(version) => self.handle_snapshot(version).await,
                    CoordinationMessage::Reconfigure((new_set, new_version)) => {
                        self.handle_reconfigure(new_set, new_version).await
                    }
                    CoordinationMessage::Suspend => {
                        self.handle_suspend().await;
                        return;
                    }
                }
            }
        })
    }

    async fn handle_snapshot(&self, version: SnapshotVersion) {
        let persistence_client = self
            .persistence_backend
            .for_version(self.worker_id, &version);
        self.coordinator_comm.send(WorkerMessage::SnapshotStarted);

        let (tx, mut rx) = mpsc::channel(1);
        let msg = SysMessage::Snapshot {
            client: persistence_client,
            callback: tx,
        };
        self.sys_msg_sender.send(msg).await;
        let _ = rx.recv().await;
        self.coordinator_comm
            .send(WorkerMessage::SnapshotComplete(version));
    }

    async fn handle_reconfigure(&self, new_set: IndexSet<WorkerId>, new_version: u64) {
        let (tx, mut rx) = mpsc::channel(1);
        let msg = SysMessage::Reconfigure {
            new_set,
            new_version,
            callback: tx,
        };
        self.sys_msg_sender.send(msg).await;
        let _ = rx.recv().await;
        self.coordinator_comm
            .send(WorkerMessage::ReconfigureComplete(new_version));
    }

    async fn handle_suspend(&self) {
        let (tx, mut rx) = mpsc::channel(1);
        let msg = SysMessage::Suspend { callback: tx };
        self.sys_msg_sender.send(msg).await;
        let _ = rx.recv().await;
        self.coordinator_comm.send(WorkerMessage::SuspendComplete);
    }
}
