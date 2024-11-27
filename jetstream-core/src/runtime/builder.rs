use std::rc::Rc;
use std::sync::Mutex;

use crate::channels::selective_broadcast::{link, merge_receiver_groups, Input};
use crate::snapshot::controller::make_snapshot_controller;
pub use crate::snapshot::controller::SnapshotTrigger;
use crate::snapshot::{PersistenceBackend, PersistenceClient};
use crate::stream::JetStreamBuilder;
use crate::stream::{
    AppendableOperator, BuildContext, BuildableOperator, OperatorBuilder,
    RunnableOperator,
};
use crate::types::{MaybeData, MaybeKey, NoData, NoKey, OperatorPartitioner, WorkerId};
use crate::types::{MaybeTime, NoTime};
use thiserror::Error;

use super::runtime_flavor::CommunicationError;
use super::{CommunicationBackend, RuntimeFlavor};

type RootOperator = OperatorBuilder<NoKey, NoData, NoTime, NoKey, NoData, NoTime>;

/// Builder for a JetStream worker.
/// The Worker is the core block of executing JetStream dataflows.
/// This builder is used to create new streams and configure the
/// execution environment.
pub struct WorkerBuilder<F, P> {
    inner: Rc<Mutex<InnerRuntimeBuilder>>,
    flavor: F,
    root_operator: RootOperator,
    persistence_client: P,
}

impl<F, P> WorkerBuilder<F, P>
where
    F: RuntimeFlavor,
{
    pub fn new<B: PersistenceBackend<Client = P>, T: SnapshotTrigger>(
        flavor: F,
        snapshot_trigger: T,
        persistence_backend: B,
    ) -> WorkerBuilder<F, P> {
        let persistence_client = persistence_backend.last_commited(flavor.this_worker_id());
        let root_operator = make_snapshot_controller(persistence_backend, snapshot_trigger);
        let inner = Rc::new(Mutex::new(InnerRuntimeBuilder {
            operators: Vec::new(),
        }));
        WorkerBuilder {
            inner,
            flavor,
            root_operator,
            persistence_client,
        }
    }
}

impl<F, P> WorkerBuilder<F, P>
where
    F: RuntimeFlavor,
    P: PersistenceClient,
{
    pub fn new_stream(&mut self) -> JetStreamBuilder<NoKey, NoData, NoTime> {
        // link our new stream to the root stream we will build later
        // so it can receive system messages
        let mut receiver = Input::new_unlinked();
        link(self.root_operator.get_output_mut(), &mut receiver);
        JetStreamBuilder::from_receiver(
            receiver,
            self.inner.clone(),
        )
    }

    pub fn build(mut self) -> Result<Worker<<F as RuntimeFlavor>::Communication>, BuildError> {
        let this_worker = self.flavor.this_worker_id();
        let state_client = Rc::new(self.persistence_client) as Rc<dyn PersistenceClient>;

        let ref_count = Rc::strong_count(&self.inner);
        let inner =
            Rc::try_unwrap(self.inner).map_err(|_| BuildError::UnfinishedStreams(ref_count - 1))?;

        let mut operators = inner.into_inner().unwrap().finish();
        operators.insert(0, Box::new(self.root_operator).into_buildable());

        let cluster_size: u64 = self.flavor.runtime_size().try_into().unwrap();
        let mut communication_backend = self.flavor.establish_communication()?;
        let operators: Vec<RunnableOperator> = operators
            .into_iter()
            .enumerate()
            .map(|(i, x)| {
                let label = x.get_label().unwrap_or(format!("operator_id_{}", i));
                let mut ctx = BuildContext::new(
                    this_worker,
                    i.try_into().expect("Too many operators"),
                    label,
                    Rc::clone(&state_client),
                    &mut communication_backend,
                    (0..cluster_size).collect(),
                );
                x.into_runnable(&mut ctx)
            })
            .collect();
        let worker = Worker {
            worker_id: this_worker,
            operators,
            communication: communication_backend,
        };

        Ok(worker)
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
}

#[derive(Default)]
pub(crate) struct InnerRuntimeBuilder {
    operators: Vec<Box<dyn BuildableOperator>>,
}
impl InnerRuntimeBuilder {
    // pub fn new_stream(&mut self) -> JetStreamBuilder<NoKey, NoData, NoTime> {
    //     let mut new_op = pass_through_operator();
    //     selective_broadcast::link(self.root_stream.get_output_mut(), new_op.get_input_mut());
    //     JetStreamBuilder::from_operator(new_op).label("jetstream::pass_through")
    // }

    pub(crate) fn add_operators(
        &mut self,
        operators: impl IntoIterator<Item=Box<dyn BuildableOperator>>,
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
    C: CommunicationBackend,
{
    pub fn step(&mut self) -> bool {
        let span = tracing::debug_span!("scheduling::run_graph", worker_id = self.worker_id);
        let _span_guard = span.enter();
        let mut all_done = true;
        for (i, op) in self.operators.iter_mut().enumerate().rev() {
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

/// A snapshot trigger, which never triggers a snapshot
pub const fn no_snapshots() -> bool {
    false
}

#[cfg(test)]
mod tests {
    use crate::snapshot::NoPersistence;

    use super::{no_snapshots, WorkerBuilder};
    use crate::runtime::threaded::SingleThreadRuntimeFlavor;

    /// check we can build the most basic runtime
    #[test]
    fn builds_basic_rt() {
        WorkerBuilder::new(
            SingleThreadRuntimeFlavor,
            no_snapshots,
            NoPersistence::default(),
        )
        .build()
        .unwrap();
    }
}
