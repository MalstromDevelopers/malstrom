use std::{collections::HashMap, rc::Rc, sync::Mutex};

use indexmap::IndexSet;
use thiserror::Error;
use tokio::{runtime::LocalRuntime, sync::mpsc};
use tracing::info;

use crate::{
    channels::signal::SignalHandle,
    coordinator::messages::*,
    runtime::{
        CommunicationError, OperatorOperatorComm, RuntimeFlavor,
        communication::{WorkerClient, WorkerCoordinatorComm},
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
    coordinator_comm: WorkerClient,
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
        operators: HashMap<u64, tokio::task::JoinHandle<()>>,
        build_ctx_sender: tokio::sync::broadcast::Sender<WorkerBuildContext>,
    ) -> Result<(), WorkerExecutionError> {
        let (msg, build_responder) = self
            .comm_rt
            .block_on(self.coordinator_comm.recv::<StartBuild, ()>());
        let buildinfo = msg.0;
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
        let _ = build_ctx_sender.send(build_ctx);
        self.comm_rt.block_on(build_responder.respond(()));

        let (_, exec_start_responder) = self
            .comm_rt
            .block_on(self.coordinator_comm.recv::<StartExecution, ()>());

        let coord_task = CoordinationTask::new(
            self.worker_id,
            self.persistence_backend,
            sys_msg_sender,
            self.coordinator_comm,
        )
        .start(&self.comm_rt);

        self.comm_rt.block_on(exec_start_responder.respond(()));

        let tasks = operators.into_values();
        operator_rt.block_on(futures::future::join_all(tasks));
        info!("Finished execution");

        Ok(())
    }
}

/// Possible errors when starting execution on the worker
#[allow(missing_docs)]
#[derive(Error, Debug)]
pub enum WorkerExecutionError {
    #[error(
        "{0} Unfinished streams in this runtime.
    You must call `.finish()` on all streams created on this runtime
    or drop them before building the Runtime"
    )]
    UnfinishedStreams(usize),
    #[error("Operator name '{0}' is not unique. Rename this operator.")]
    NonUniqueName(String),
    #[error("Error starting async runtime")]
    AsyncRuntime(#[from] std::io::Error),
    #[error("Error in communication backend")]
    Communication(#[from] CommunicationError),
}
