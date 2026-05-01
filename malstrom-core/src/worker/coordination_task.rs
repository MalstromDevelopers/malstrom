use std::{collections::HashMap, rc::Rc, sync::Mutex};

use indexmap::IndexSet;
use thiserror::Error;
use tokio::{runtime::LocalRuntime, sync::mpsc};
use tracing::info;

use crate::{
    channels::signal::SignalHandle,
    coordinator::messages::{BuildInformation, RuntimeMessage},
    runtime::{
        OperatorOperatorComm, RuntimeFlavor,
        communication::{WorkerClient, WorkerCoordinatorComm},
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
    coordinator_comm: WorkerClient,
}

impl<P> CoordinationTask<P>
where
    P: PersistenceBackend,
{
    pub(super) fn new(
        this_worker: WorkerId,
        persistence_backend: P,
        sys_msg_sender: mpsc::Sender<SysMessage<P::Client>>,
        coordinator_comm: WorkerClient,
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
                let (msg, responder) = self.coordinator_comm.recv::<RuntimeMessage, bool>().await;
                match msg {
                    RuntimeMessage::Snapshot(version) => {
                        self.handle_snapshot(version).await;
                        responder.respond(true).await;
                    }
                    RuntimeMessage::Reconfigure((new_set, new_version)) => {
                        self.handle_reconfigure(new_set, new_version).await;
                        responder.respond(true).await;
                    }
                    RuntimeMessage::Suspend => {
                        self.handle_suspend().await;
                        responder.respond(true).await;
                        return;
                    }
                    RuntimeMessage::ExecutionComplete => {
                        // following task already dropped
                        let finished = self.sys_msg_sender.is_closed();
                        responder.respond(finished).await;
                        if finished {
                            return;
                        }
                    }
                }
            }
        })
    }

    async fn handle_snapshot(&self, version: SnapshotVersion) {
        let persistence_client = self
            .persistence_backend
            .for_version(self.worker_id, &version);

        let (tx, mut rx) = mpsc::channel(1);
        let msg = SysMessage::Snapshot {
            client: persistence_client,
            callback: tx,
        };
        self.sys_msg_sender.send(msg).await;
        let _ = rx.recv().await;
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
    }

    async fn handle_suspend(&self) {
        let (tx, mut rx) = mpsc::channel(1);
        let msg = SysMessage::Suspend { callback: tx };
        self.sys_msg_sender.send(msg).await;
        let _ = rx.recv().await;
    }
}
