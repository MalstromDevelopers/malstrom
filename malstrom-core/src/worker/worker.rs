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
    worker::{
        InnerRuntimeBuilder, coordination_task::CoordinationTask, root_logic::RootLogic,
        sys_message::SysMessage,
    },
};

pub struct Worker<P, C> {
    persistence_backend: P,
    communication_backend: Rc<C>,
    coordinator_comm: CommunicationClient<WorkerMessage, CoordinationMessage>,
    comm_rt: tokio::runtime::Runtime,
    worker_id: WorkerId,
}

impl<P, C> Worker<P, C>
where
    P: PersistenceBackend,
    C: OperatorOperatorComm + WorkerCoordinatorComm + 'static,
{
    pub(super) fn new(
        persistence_backend: P,
        communication_backend: C,
        worker_id: WorkerId,
    ) -> Result<Self, WorkerExecutionError> {
        let comm_rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        let coordinator_comm = CommunicationClient::worker_to_coordinator(&communication_backend)?;

        Ok(Self {
            persistence_backend,
            communication_backend: Rc::new(communication_backend),
            coordinator_comm,
            comm_rt,
            worker_id,
        })
    }

    pub(super) fn execute(
        self,
        sys_msg_sender: mpsc::Sender<SysMessage<P::Client>>,
        operator_rt: LocalRuntime,
        operators: HashMap<u64, (tokio::task::JoinHandle<()>, SignalHandle)>,
        build_ctx_sender: tokio::sync::broadcast::Sender<WorkerBuildContext>,
    ) -> Result<(), WorkerExecutionError> {
        let buildinfo = self.wait_for_build_info();
        info!("Obtained build info: {:?}", buildinfo);

        let state_client = match buildinfo.resume_snapshot {
            Some(v) => Rc::new(self.persistence_backend.for_version(self.worker_id, &v))
                as Rc<dyn PersistenceClient>,
            None => Rc::new(NoPersistence) as Rc<dyn PersistenceClient>,
        };
        let build_ctx = WorkerBuildContext::new(
            self.worker_id,
            Rc::clone(&state_client),
            Rc::clone(&self.communication_backend) as Rc<dyn OperatorOperatorComm>,
            buildinfo.worker_set.clone(),
        );

        self.coordinator_comm.send(WorkerMessage::BuildComplete);
        self.wait_for_execution_start();

        let _ = build_ctx_sender.send(build_ctx);

        let coord_task = CoordinationTask::new(
            self.worker_id,
            self.persistence_backend,
            sys_msg_sender,
            self.coordinator_comm,
        )
        .start(&self.comm_rt);
        let (_tasks, signals): (Vec<_>, Vec<_>) = operators.into_values().collect();
        operator_rt.block_on(futures::future::join_all(
            signals.into_iter().map(SignalHandle::watch),
        ));
        info!("Finished execution");
        Ok(())
    }

    fn wait_for_build_info(&self) -> BuildInformation {
        info!("Waiting for Coordinator build info");
        self.comm_rt.block_on(async move {
            match self.coordinator_comm.recv_async().await {
                CoordinationMessage::StartBuild(buildinfo) => buildinfo,
                _ => unreachable!(),
            }
        })
    }

    fn wait_for_execution_start(&self) {
        self.comm_rt.block_on(async move {
            match self.coordinator_comm.recv_async().await {
                CoordinationMessage::StartExecution => (),
                _ => unreachable!(),
            }
        })
    }
}

/// Possible errors when starting execution on the worker
#[allow(missing_docs)]
#[derive(Error, Debug)]
pub enum WorkerExecutionError {
    #[error("Error establishing communication to workers/coordinator")]
    CommunicationError(#[from] crate::runtime::CommunicationError),
    #[error("Error from communication backend")]
    CommunicationBackendError(#[from] crate::runtime::communication::CommunicationBackendError),
    #[error(
        "{0} Unfinished streams in this runtime.
    You must call `.finish()` on all streams created on this runtime
    or drop them before building the Runtime"
    )]
    UnfinishedStreams(usize),
    #[error("Operator name '{0}' is not unique. Rename this operator.")]
    NonUniqueName(String),
    #[error("Error starting async runtime: {0:?}")]
    AsyncRuntime(#[from] std::io::Error),
    #[error(transparent)]
    Coordinator(#[from] crate::coordinator::CoordinatorExecutionError),
}
