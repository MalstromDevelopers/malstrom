//! Worker: A worker in Malstrom is the unit which is executing the operations in job. A Worker is
//! also the unit of parellism i.e. the Malstrom runtime will create as many **identical** workers
//! as the configured parallelism requires.
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::sync::Mutex;

use crate::channels::operator_io::{Input, Output, RootOutput, full_broadcast, link, link_root};
use crate::coordinator::CoordinatorExecutionError;
use crate::coordinator::types::{CoordinationMessage, WorkerMessage};
use crate::snapshot::{Barrier, NoPersistence, PersistenceBackend, PersistenceClient};
use crate::stream::{BuildContext, DirectLogic, Logic, LogicBuilder, Operator, WorkerBuildContext};
use crate::stream::{InitialStreamBuilder, StreamBuilder};
use crate::types::{
    Kvt, MaybeData, MaybeKey, Message, NoData, NoKey, OperatorId, RescaleMessage, SuspendMarker,
    WorkerId,
};
use crate::types::{MaybeTime, NoTime};
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt, TryFutureExt};
use indexmap::IndexSet;
use thiserror::Error;
use tokio::runtime::LocalRuntime;
use tokio::sync::{broadcast, mpsc, watch};
use tracing::{Level, info, span};

use crate::runtime::communication::CommunicationBackendError;
use crate::runtime::runtime_flavor::CommunicationError;
use crate::runtime::{CommunicationClient, OperatorOperatorComm, RuntimeFlavor};

/// Builder for a Malstrom worker.
/// The Worker is the core block of executing JetStream dataflows.
/// This builder is used to create new streams and configure the
/// execution environment.
pub struct WorkerBuilder<F, P: PersistenceBackend> {
    inner: Rc<Mutex<InnerRuntimeBuilder>>,
    flavor: F,
    persistence: P,
    // root operator
    root_operator: Operator<(), DirectLogic<RootLogic<P::Client>>, ()>,
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
            Operator::<(), _, ()>::direct("malstrom::root".to_string(), RootLogic(rx));

        let inner = Rc::new(Mutex::new(InnerRuntimeBuilder {
            build_ctx: broadcast::Sender::new(1),
            operator_rt: LocalRuntime::new().unwrap(),
            operator_tasks: HashMap::new(),
        }));

        WorkerBuilder {
            inner,
            flavor,
            persistence,
            root_operator,
            sys_msg_sender: tx,
        }
    }
}

/// Creates new streams to add to the job
pub trait StreamProvider {
    /// Create a new empty stream. This stream will not contain any data.
    /// Call `.source()` on the stream to add a source.
    fn new_stream(&mut self) -> InitialStreamBuilder;
}

impl<F, P> StreamProvider for WorkerBuilder<F, P>
where
    P: PersistenceBackend,
{
    fn new_stream(&mut self) -> InitialStreamBuilder {
        // link our new stream to the root stream we will build later
        // so it can receive system messages
        let mut input = Input::new_unlinked();
        link(&mut self.root_operator.output, &mut input);
        InitialStreamBuilder::new(input, self.inner.clone())
    }
}

impl<F, P> WorkerBuilder<F, P>
where
    F: RuntimeFlavor,
    P: PersistenceBackend,
{
    /// Start job execution on this worker.
    ///
    /// The worker will wait for instruction from the Coordinator to build the dataflow and then
    /// commence execution.
    /// This method returns when execution is completed or suspended.
    pub fn execute(mut self) -> Result<(), WorkerExecutionError> {
        let this_worker = self.flavor.this_worker_id();
        // runtime used for communication with other workers
        let comm_rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();

        let ref_count = Rc::strong_count(&self.inner);
        let mut inner = Rc::try_unwrap(self.inner)
            .map_err(|_| WorkerExecutionError::UnfinishedStreams(ref_count - 1))?
            .into_inner()
            .unwrap();

        let root_op_id = inner.add_operator(self.root_operator);

        let mut communication_backend = self.flavor.communication()?;
        let coordinator = CommunicationClient::worker_to_coordinator(&communication_backend)?;

        info!("Waiting for Coordinator build info");
        let (buildinfo, coordinator) = comm_rt.block_on(async move {
            match coordinator.recv_async().await {
                CoordinationMessage::StartBuild(buildinfo) => (buildinfo, coordinator),
                _ => unreachable!(),
            }
        });

        info!("Obtained build info: {:?}", buildinfo);
        let state_client = match buildinfo.resume_snapshot {
            Some(v) => {
                Rc::new(self.persistence.for_version(this_worker, &v)) as Rc<dyn PersistenceClient>
            }
            None => Rc::new(NoPersistence) as Rc<dyn PersistenceClient>,
        };
        let build_ctx = WorkerBuildContext::new(
            this_worker,
            Rc::clone(&state_client),
            Rc::new(communication_backend),
            buildinfo.worker_set.clone(),
        );
        coordinator.send(WorkerMessage::BuildComplete);

        let coordinator = comm_rt.block_on(async move {
            match coordinator.recv_async().await {
                CoordinationMessage::StartExecution => coordinator,
                _ => unreachable!(),
            }
        });
        let _ = inner.build_ctx.send(build_ctx);
        // run all operators
        let root_operator = inner.operator_tasks.remove(&root_op_id).expect("Root operator must exist");
        let mut operators: FuturesUnordered<_> = inner.operator_tasks.into_iter().map(async |(k, v)| (k, v.await)).collect();

        let persistence = self.persistence;
        let sys_msg_sender = self.sys_msg_sender;
        let coordination = comm_rt.spawn(async move {
            loop {
                let msg = coordinator.recv_async().await;
                match msg {
                    CoordinationMessage::StartBuild(_) => unreachable!(),
                    CoordinationMessage::StartExecution => unreachable!(),
                    CoordinationMessage::Snapshot(version) => {
                        let persistence_client = persistence.for_version(this_worker, &version);
                        coordinator.send(WorkerMessage::SnapshotStarted);

                        let (tx, mut rx) = mpsc::channel(1);
                        let msg = SysMessage::Snapshot {
                            client: persistence_client,
                            callback: tx,
                        };
                        sys_msg_sender.send(msg).await;
                        // wait for last barrier to be dropped
                        let _ = rx.recv().await;
                        coordinator.send(WorkerMessage::SnapshotComplete(version));
                    }
                    CoordinationMessage::Reconfigure((new_set, new_version)) => {
                        let (tx, mut rx) = mpsc::channel(1);
                        let msg = SysMessage::Reconfigure {
                            new_set,
                            new_version,
                            callback: tx,
                        };
                        sys_msg_sender.send(msg).await;
                        let _ = rx.recv().await;
                        coordinator.send(WorkerMessage::ReconfigureComplete(new_version));
                    }
                    CoordinationMessage::Suspend => {
                        let (tx, mut rx) = mpsc::channel(1);
                        let msg = SysMessage::Suspend { callback: tx };
                        sys_msg_sender.send(msg).await;
                        let _ = rx.recv().await;
                        coordinator.send(WorkerMessage::SuspendComplete);
                        return;
                    }
                }
            }
        });
        
        println!("Running {} operators exluding root", operators.len());
        while let Some((id, res)) = inner.operator_rt.block_on(operators.next()) {
            res.unwrap();
            println!("{id} finished")
        }        
        println!("All operators finished");
        info!("Finished execution");
        Ok(())
    }
}

/// The standard message is not Send so we use this
/// type to send messages from the coordination runtime
/// to the operator runtime
pub(crate) enum SysMessage<P> {
    Snapshot {
        client: P,
        callback: mpsc::Sender<()>,
    },
    Reconfigure {
        new_set: IndexSet<WorkerId>,
        new_version: u64,
        callback: mpsc::Sender<()>,
    },
    Suspend {
        callback: mpsc::Sender<()>,
    },
}

/// Possible errors when starting execution on the worker
#[allow(missing_docs)]
#[derive(Error, Debug)]
pub enum WorkerExecutionError {
    #[error("Error establishing communication to workers/coordinator")]
    CommunicationError(#[from] CommunicationError),
    #[error("Error from communication backend")]
    CommunicationBackendError(#[from] CommunicationBackendError),
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
    Coordinator(#[from] CoordinatorExecutionError),
}

pub(crate) struct InnerRuntimeBuilder {
    // build_ctx will be sent here once available
    build_ctx: broadcast::Sender<WorkerBuildContext>,
    operator_rt: LocalRuntime,
    operator_tasks: HashMap<OperatorId, tokio::task::JoinHandle<()>>,
}

impl InnerRuntimeBuilder {
    pub(crate) fn add_operator<In, B, Out>(&mut self, operator: Operator<In, B, Out>) -> OperatorId
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

struct RootLogic<P>(mpsc::Receiver<SysMessage<P>>);
impl<P: PersistenceClient> Logic<(), ()> for RootLogic<P> {
    async fn apply(
        &mut self,
        input: &mut Input<()>,
        output: &mut Output<()>,
        ctx: &mut crate::stream::OperatorContext,
    ) {
        while let Some(sys_msg) = self.0.recv().await {
            match sys_msg {
                SysMessage::Snapshot { client, callback } => {
                    let barrier = Barrier::new(Box::new(client), callback);
                    output.send(Message::AbsBarrier(barrier)).await;
                }
                SysMessage::Reconfigure {
                    new_set,
                    new_version,
                    callback,
                } => {
                    let reconfig = RescaleMessage::new(new_set, new_version, callback);
                    output.send(Message::Rescale(reconfig)).await;
                }
                SysMessage::Suspend { callback } => {
                    let suspend = SuspendMarker::new(callback);
                    output.send(Message::SuspendMarker(suspend)).await;
                }
            }
        }
    }
}
