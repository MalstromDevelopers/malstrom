use std::collections::HashSet;
use std::rc::Rc;
use std::sync::Mutex;

use crate::channels::operator_io::{full_broadcast, link, merge_receiver_groups, Input, Output};
use crate::coordinator::messages::WorkerClient;
use crate::snapshot::{PersistenceBackend, PersistenceClient};
use crate::stream::JetStreamBuilder;
use crate::stream::{
    BuildContext, BuildableOperator, OperatorBuilder, RunnableOperator,
};
use crate::types::{MaybeData, MaybeKey, NoData, NoKey, WorkerId};
use crate::types::{MaybeTime, NoTime};
use thiserror::Error;

use super::rescaling::{RescaleError, RescaleRequest};
use super::runtime_flavor::CommunicationError;
use super::{RuntimeFlavor, OperatorOperatorComm};

type RootOperator = OperatorBuilder<NoKey, NoData, NoTime, NoKey, NoData, NoTime>;

/// Builder for a JetStream worker.
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
    P: PersistenceBackend
{
    pub fn new(
        flavor: F,
        persistence: P
    ) -> WorkerBuilder<F, P> {
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

pub trait StreamProvider {
    fn new_stream(&mut self) -> JetStreamBuilder<NoKey, NoData, NoTime>;
}

impl<F, P> StreamProvider for WorkerBuilder<F, P> {
    fn new_stream(&mut self) -> JetStreamBuilder<NoKey, NoData, NoTime> {
        // link our new stream to the root stream we will build later
        // so it can receive system messages
        let mut receiver = Input::new_unlinked();
        link(&mut self.root_stream, &mut receiver);
        JetStreamBuilder::from_receiver(receiver, self.inner.clone())
    }
}

impl<F, P> WorkerBuilder<F, P>
where
    F: RuntimeFlavor,
    P: PersistenceBackend,
{

    pub fn build_and_run(mut self) -> Result<(), BuildError> {
        let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build()?;
        let ref_count = Rc::strong_count(&self.inner);
        let inner =
            Rc::try_unwrap(self.inner).map_err(|_| BuildError::UnfinishedStreams(ref_count - 1))?;
        
        let this_worker = self.flavor.this_worker_id();
        
        let mut communication_backend = self.flavor.communication()?;
        let coordinator = WorkerClient::new(&communication_backend);
        let (build_info, coordinator) = rt.block_on(coordinator.get_build_info());
        let cluster_size = build_info.scale;
        let state_client = Rc::new(self.persistence.for_version(this_worker, &build_info.resume_snapshot)) as Rc<dyn PersistenceClient>;

        let mut operators = vec![];
        operators.extend(inner.into_inner().unwrap().finish().into_iter());

        let mut seen_ids = HashSet::new();
        let operators: Vec<RunnableOperator> = operators
            .into_iter()
            .enumerate()
            .map(|(i, x)| {
                let operator_name = x.get_name().to_string();
                let operator_id = x.get_id();
                if !seen_ids.insert(operator_id) {
                    Err(BuildError::NonUniqueName(operator_name.clone()))?
                }
                let mut ctx = BuildContext::new(
                    this_worker,
                    operator_id,
                    operator_name,
                    Rc::clone(&state_client),
                    &mut communication_backend,
                    (0..cluster_size).collect(),
                );
                Result::<RunnableOperator, BuildError>::Ok(x.into_runnable(&mut ctx))
            })
            .collect::<Result<Vec<RunnableOperator>, BuildError>>()?;
        let mut worker = Worker {
            worker_id: this_worker,
            operators,
            communication: communication_backend,
        };
        
        let coordinator = rt.block_on(coordinator.await_start());
        worker.execute();
        Ok(())
    }
}

#[derive(Error, Debug)]
pub enum BuildError {
    #[error(transparent)]
    CommunicationError(#[from] CommunicationError),
    #[error(
        "{0} Unfinished streams in this runtime.
    You must call `.finish()` on all streams created on this runtime
    or drop them before building the Runtime"
    )]
    UnfinishedStreams(usize),
    #[error("Operator name '{0}' is not unique. Rename this operator.")]
    NonUniqueName(String),
    #[error("Error starting async runtime: {0:?}")]
    AsyncRuntime(#[from] std::io::Error)
}

#[derive(Default)]
pub(crate) struct InnerRuntimeBuilder {
    operators: Vec<Box<dyn BuildableOperator>>,
}
impl InnerRuntimeBuilder {
    // pub fn new_stream(&mut self) -> JetStreamBuilder<NoKey, NoData, NoTime> {
    //     let mut new_op = pass_through_operator();
    //     selective_broadcast::link(self.root_stream.get_output_mut(), new_op.get_input_mut());
    //     JetStreamBuilder::from_operator(new_op).label("malstrom::pass_through")
    // }

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
    streams: impl Iterator<Item = JetStreamBuilder<K, V, T>>,
) -> JetStreamBuilder<K, V, T> {
    let stream_receivers = streams.map(|x| x.finish_pop_tail()).collect();
    let merged = merge_receiver_groups(stream_receivers);
    JetStreamBuilder::from_receiver(merged, runtime)
}

pub struct Worker<C> {
    worker_id: WorkerId,
    operators: Vec<RunnableOperator>,
    communication: C,
}
impl<C> Worker<C>
where
    C: OperatorOperatorComm,
{
    pub fn step(&mut self) -> bool {
        let span = tracing::debug_span!("scheduling::run_graph", worker_id = self.worker_id);
        let _span_guard = span.enter();
        let mut all_done = true;
        for (i, op) in self.operators.iter_mut().enumerate().rev() {
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
    pub fn execute(&mut self) {
        while !self.step() {}
    }
}

#[derive(Clone)]
pub struct WorkerApiHandle {
    rescale_tx: std::sync::mpsc::Sender<RescaleRequest>,
}

impl WorkerApiHandle {
    pub async fn rescale(&self, change: i64) -> Result<(), RescaleError> {
        if change == 0 {
            return Ok(());
        }
        let (req, callback) = RescaleRequest::new(change);
        // PANIC: controller impl guarantees sender/receiver wont be dropped
        self.rescale_tx.send(req).unwrap();
        // TODO // callback.await.unwrap()
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::threaded::SingleThreadRuntimeFlavor;
    use crate::snapshot::{NoPersistence, NoSnapshots};

    /// check we can build the most basic runtime
    #[test]
    fn builds_basic_rt() {
        WorkerBuilder::new(
            SingleThreadRuntimeFlavor,
            NoSnapshots,
            NoPersistence::default(),
        )
        .build()
        .unwrap();
    }
}
