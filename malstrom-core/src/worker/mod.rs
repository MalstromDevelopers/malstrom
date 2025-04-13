//! Worker: A worker in Malstrom is the unit which is executing the operations in job. A Worker is
//! also the unit of parellism i.e. the Malstrom runtime will create as many **identical** workers
//! as the configured parallelism requires.
use std::collections::HashSet;
use std::rc::Rc;
use std::sync::Mutex;

use crate::channels::operator_io::{full_broadcast, link, merge_inputs, Input, Output};
use crate::coordinator::types::{CoordinationMessage, WorkerMessage};
use crate::coordinator::CoordinatorExecutionError;
use crate::snapshot::{Barrier, NoPersistence, PersistenceBackend, PersistenceClient};
use crate::stream::StreamBuilder;
use crate::stream::{BuildContext, BuildableOperator, RunnableOperator};
use crate::types::{
    MaybeData, MaybeKey, Message, NoData, NoKey, RescaleMessage, SuspendMarker, WorkerId,
};
use crate::types::{MaybeTime, NoTime};
use indexmap::IndexSet;
use thiserror::Error;
use tracing::{info, span, Level};

use crate::runtime::communication::CommunicationBackendError;
use crate::runtime::runtime_flavor::CommunicationError;
use crate::runtime::{CommunicationClient, OperatorOperatorComm, RuntimeFlavor};

/// Builder for a Malstrom worker.
/// The Worker is the core block of executing JetStream dataflows.
/// This builder is used to create new streams and configure the
/// execution environment.
pub struct WorkerBuilder<F, P> {
    inner: Rc<Mutex<InnerRuntimeBuilder>>,
    flavor: F,
    persistence: P,
    root_stream: Output<NoKey, NoData, NoTime>,
}

impl<F, P> WorkerBuilder<F, P>
where
    F: RuntimeFlavor,
    P: PersistenceBackend,
{
    /// Create a new Worker with the given runtime and persistence backend.
    pub fn new(flavor: F, persistence: P) -> WorkerBuilder<F, P> {
        let inner = Rc::new(Mutex::new(InnerRuntimeBuilder {
            operators: Vec::new(),
        }));

        WorkerBuilder {
            inner,
            flavor,
            persistence,
            root_stream: Output::new_unlinked(full_broadcast),
        }
    }
}

/// Creates new streams to add to the job
pub trait StreamProvider {
    /// Create a new empty stream. This stream will not contain any data.
    /// Call `.source()` on the stream to add a source.
    fn new_stream(&mut self) -> StreamBuilder<NoKey, NoData, NoTime>;
}

impl<F, P> StreamProvider for WorkerBuilder<F, P> {
    fn new_stream(&mut self) -> StreamBuilder<NoKey, NoData, NoTime> {
        // link our new stream to the root stream we will build later
        // so it can receive system messages
        let mut receiver = Input::new_unlinked();
        link(&mut self.root_stream, &mut receiver);
        StreamBuilder::from_receiver(receiver, self.inner.clone())
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
        let _span = span!(Level::INFO, "worker", worker_id = this_worker);
        let _span_guard = _span.enter();
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        let ref_count = Rc::strong_count(&self.inner);
        let inner = Rc::try_unwrap(self.inner)
            .map_err(|_| WorkerExecutionError::UnfinishedStreams(ref_count - 1))?;

        let mut communication_backend = self.flavor.communication()?;
        let coordinator = CommunicationClient::worker_to_coordinator(&communication_backend)?;

        info!("Waiting for Coordinator build info");
        let (buildinfo, coordinator) = rt.block_on(async move {
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

        let mut operators = vec![];
        #[allow(clippy::unwrap_used)]
        operators.extend(inner.into_inner().unwrap().finish());

        let mut seen_ids = HashSet::new();
        let operators: Vec<RunnableOperator> = operators
            .into_iter()
            .map(|x| {
                let operator_name = x.get_name().to_string();
                let operator_id = x.get_id();
                if !seen_ids.insert(operator_id) {
                    Err(WorkerExecutionError::NonUniqueName(operator_name.clone()))?
                }
                let mut ctx = BuildContext::new(
                    this_worker,
                    operator_id,
                    operator_name,
                    Rc::clone(&state_client),
                    &mut communication_backend,
                    buildinfo.worker_set.clone(),
                );
                Result::<RunnableOperator, WorkerExecutionError>::Ok(x.into_runnable(&mut ctx))
            })
            .collect::<Result<Vec<RunnableOperator>, WorkerExecutionError>>()?;
        let mut worker = Worker {
            worker_id: this_worker,
            operators,
            communication: communication_backend,
            persistence: self.persistence,
        };
        coordinator.send(WorkerMessage::BuildComplete);

        let coordinator = rt.block_on(async move {
            match coordinator.recv_async().await {
                CoordinationMessage::StartExecution => coordinator,
                _ => unreachable!(),
            }
        });
        let _coordinator = worker.execute(&mut self.root_stream, coordinator);
        info!("Finished execution");
        Ok(())
    }
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

#[derive(Default)]
pub(crate) struct InnerRuntimeBuilder {
    operators: Vec<Box<dyn BuildableOperator>>,
}
impl InnerRuntimeBuilder {
    pub(crate) fn add_operators(
        &mut self,
        operators: impl IntoIterator<Item = Box<dyn BuildableOperator>>,
    ) {
        self.operators.extend(operators)
    }
    // destroy this builder and return the operators
    fn finish(self) -> Vec<Box<dyn BuildableOperator>> {
        self.operators
    }
}

/// Unions N streams with identical output types into a single stream
pub(crate) fn union<K: MaybeKey, V: MaybeData, T: MaybeTime>(
    runtime: Rc<Mutex<InnerRuntimeBuilder>>,
    streams: impl Iterator<Item = StreamBuilder<K, V, T>>,
) -> StreamBuilder<K, V, T> {
    let stream_receivers = streams.map(|x| x.finish_pop_tail()).collect();
    let merged = merge_inputs(stream_receivers);
    StreamBuilder::from_receiver(merged, runtime)
}

struct Worker<C, P> {
    worker_id: WorkerId,
    operators: Vec<RunnableOperator>,
    communication: C,
    persistence: P,
}
impl<C, P> Worker<C, P>
where
    C: OperatorOperatorComm,
    P: PersistenceBackend,
{
    fn step(&mut self) -> bool {
        let span = tracing::debug_span!("scheduling::run_graph", worker_id = self.worker_id);
        let _span_guard = span.enter();
        let mut all_done = true;
        for op in self.operators.iter_mut().rev() {
            let span = span!(Level::INFO, "operator", operator_name = op.name());
            let _span_guard = span.enter();
            if op.is_suspended() {
                continue;
            }
            op.step(&mut self.communication);
            while op.has_queued_work() {
                op.step(&mut self.communication);
            }
            all_done &= op.is_finalized();
        }
        all_done
    }

    /// Repeatedly schedule all operators until all have reached a finished state
    /// Note that depending on the specific operator implementations this may never
    /// be the case
    fn execute(
        &mut self,
        root: &mut Output<NoKey, NoData, NoTime>,
        coordinator: CommunicationClient<WorkerMessage, CoordinationMessage>,
    ) -> CommunicationClient<WorkerMessage, CoordinationMessage> {
        coordinator.send(WorkerMessage::ExecutionStarted);
        while !self.step() {
            if let Some(msg) = coordinator.recv() {
                match msg {
                    CoordinationMessage::StartBuild(_) => unreachable!(),
                    CoordinationMessage::StartExecution => unreachable!(),
                    CoordinationMessage::Snapshot(version) => {
                        let persistence_client =
                            self.persistence.for_version(self.worker_id, &version);
                        coordinator.send(WorkerMessage::SnapshotStarted);
                        perform_snapshot(root, persistence_client, &mut || self.step());
                        coordinator.send(WorkerMessage::SnapshotComplete(version));
                    }
                    CoordinationMessage::Reconfigure((index_set, version)) => {
                        let should_continue = index_set.contains(&self.worker_id);
                        coordinator.send(WorkerMessage::ReconfigurationStarted);
                        perform_reconfig(root, index_set, version, &mut || self.step());
                        coordinator.send(WorkerMessage::ReconfigureComplete(version));
                        if !should_continue {
                            coordinator.send(WorkerMessage::Removed);
                            return coordinator;
                        }
                    }
                    CoordinationMessage::Suspend => {
                        perform_suspend(root, &mut || self.step());
                        coordinator.send(WorkerMessage::SuspendComplete);
                        return coordinator;
                    }
                }
            }
        }
        coordinator.send(WorkerMessage::ExecutionComplete);
        coordinator
    }
}

fn perform_reconfig(
    output: &mut Output<NoKey, NoData, NoTime>,
    new_set: IndexSet<WorkerId>,
    new_version: u64,
    schedule_fn: &mut impl FnMut() -> bool,
) {
    let in_progress_rescale = RescaleMessage::new(new_set, new_version);
    output.send(Message::Rescale(in_progress_rescale.clone()));
    while in_progress_rescale.strong_count() > 1 {
        schedule_fn();
    }
}

fn perform_snapshot<P>(
    output: &mut Output<NoKey, NoData, NoTime>,
    persistence_client: P,
    schedule_fn: &mut impl FnMut() -> bool,
) where
    P: PersistenceClient,
{
    let barrier = Barrier::new(Box::new(persistence_client));
    output.send(Message::AbsBarrier(barrier.clone()));
    while barrier.strong_count() > 1 {
        schedule_fn();
    }
}

fn perform_suspend(
    output: &mut Output<NoKey, NoData, NoTime>,
    schedule_fn: &mut impl FnMut() -> bool,
) {
    let suspend = SuspendMarker::default();
    output.send(Message::SuspendMarker(suspend.clone()));
    while suspend.strong_count() > 1 {
        schedule_fn();
    }
}
