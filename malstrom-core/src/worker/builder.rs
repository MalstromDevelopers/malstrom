use std::{collections::HashMap, rc::Rc, sync::Mutex};

use futures::FutureExt as _;
use indexmap::IndexSet;
use thiserror::Error;
use tokio::{runtime::LocalRuntime, sync::mpsc};
use tracing::info;

use crate::{
    channels::signal::SignalHandle,
    coordinator::messages::BuildInformation,
    runtime::{OperatorOperatorComm, RuntimeFlavor, communication::WorkerCoordinatorComm},
    snapshot::{NoPersistence, PersistenceBackend, PersistenceClient, SnapshotVersion},
    stream::{DirectLogic, LogicBuilder, Operator, WorkerBuildContext},
    types::{Kvt, OperatorId, WorkerId},
    worker::{Worker, WorkerExecutionError, root_logic::RootLogic, sys_message::SysMessage},
};

/// Builder for a Malstrom worker.
/// The Worker is the core block of executing JetStream dataflows.
/// This builder is used to create new streams and configure the
/// execution environment.
pub struct WorkerBuilder<F, P: PersistenceBackend> {
    pub(crate) inner: Rc<Mutex<InnerRuntimeBuilder>>,
    flavor: F,
    persistence: P,
    // root operator
    pub(crate) root_operator: Operator<(), DirectLogic<RootLogic<P::Client>>, ()>,
    // at runtime system messages will be sent here to enter all streams
    sys_msg_sender: mpsc::Sender<SysMessage<P::Client>>,
}

impl<F, P> WorkerBuilder<F, P>
where
    F: RuntimeFlavor,
    P: PersistenceBackend,
{
    /// Create a new Worker with the given runtime and persistence backend.
    pub fn new(flavor: F, persistence: P) -> WorkerBuilder<F, P> {
        let (tx, rx) = mpsc::channel::<SysMessage<P::Client>>(10);
        // takes care of forwarding system messages

        let mut root_operator =
            Operator::<(), _, ()>::direct("malstrom::root".to_string(), RootLogic::new(rx));

        let inner = Rc::new(Mutex::new(InnerRuntimeBuilder::new()));
        WorkerBuilder {
            inner,
            flavor,
            persistence,
            root_operator,
            sys_msg_sender: tx,
        }
    }

    pub fn execute(mut self) -> Result<(), WorkerExecutionError> {
        let mut inner = Rc::try_unwrap(self.inner)
            .map_err(|rc| WorkerExecutionError::UnfinishedStreams(Rc::strong_count(&rc) - 1))?
            .into_inner()
            .expect("Lock poisened");
        inner.add_operator(self.root_operator);

        let worker = inner.operator_rt.block_on(Worker::new(
            self.persistence,
            self.flavor.communication()?,
            self.flavor.this_worker_id(),
        ))?;
        worker.execute(
            self.sys_msg_sender,
            inner.operator_rt,
            inner.operator_tasks,
            inner.build_ctx,
        )
    }
}

pub(crate) struct InnerRuntimeBuilder {
    // build_ctx will be sent here once available
    // TODO: replace with [tokio::sync::OnceCell]
    build_ctx: tokio::sync::broadcast::Sender<WorkerBuildContext>,
    operator_rt: Rc<LocalRuntime>,
    operator_tasks: HashMap<OperatorId, tokio::task::JoinHandle<()>>,
}

impl InnerRuntimeBuilder {
    fn new() -> Self {
        Self {
            build_ctx: tokio::sync::broadcast::Sender::new(1),
            operator_rt: Rc::new(LocalRuntime::new().unwrap()),
            operator_tasks: HashMap::new(),
        }
    }

    pub(crate) fn add_operator<In, B, Out>(
        &mut self,
        mut operator: Operator<In, B, Out>,
    ) -> OperatorId
    where
        In: Kvt,
        B: LogicBuilder<In, Out>,
        Out: Kvt,
    {
        let mut ctx_receiver = self.build_ctx.subscribe();
        let operator_id = operator.get_id();
        let operator_name = operator.get_name().to_owned();

        let task = self.operator_rt.spawn_local(async move {
            let build_ctx = ctx_receiver.recv().map(Result::unwrap);
            operator.start(build_ctx).await;
        });
        if let Some(_) = self.operator_tasks.insert(operator_id, task) {
            panic!("Non unique operator name: {operator_name}")
        }
        operator_id
    }
}
