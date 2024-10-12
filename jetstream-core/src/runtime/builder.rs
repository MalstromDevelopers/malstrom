
use std::rc::Rc;
use std::sync::Mutex;

use crate::channels::selective_broadcast::{self, Sender};
use crate::operators::void::Void;
use crate::snapshot::controller::make_snapshot_controller;
use crate::snapshot::{NoPersistence, PersistenceBackend};
use crate::stream::JetStreamBuilder;
use crate::stream::{
    pass_through_operator, AppendableOperator, BuildContext, BuildableOperator, OperatorBuilder,
    RunnableOperator,
};
use crate::types::{MaybeTime, NoTime};
use crate::types::{MaybeData, MaybeKey, NoData, NoKey, OperatorPartitioner, WorkerId};
use thiserror::Error;

use super::runtime_flavor::CommunicationError;
use super::{CommunicationBackend, RuntimeFlavor};

/// Builder for a JetStream worker.
/// The Worker is the core block of executing JetStream dataflows.
/// This builder is used to create new streams and configure the
/// execution environment.
pub struct WorkerBuilder<F, P> {
    // root_stream: JetStreamBuilder<NoKey, NoData, NoTime>,
    persistence_backend: Rc<P>,
    inner: Rc<Mutex<InnerRuntimeBuilder>>,
    flavor: F,
    snapshot_timer: Box<dyn FnMut() -> bool>,
    // this is the sender which will send system messages to
    // all streams
    root_stream_sender: Sender<NoKey, NoData, NoTime>,
}

impl<F> WorkerBuilder<F, NoPersistence>
where
    F: RuntimeFlavor,
{
    pub fn new(flavor: F) -> WorkerBuilder<F, NoPersistence> {
        let persistence_backend = Rc::new(NoPersistence::default());
        // let snapshot_op = make_snapshot_controller(persistence_backend.clone(), snapshot_timer);

        let inner = Rc::new(Mutex::new(InnerRuntimeBuilder {
            operators: Vec::new(),
        }));
        // let root_stream = JetStreamBuilder::from_operator(snapshot_op, inner.clone())
        //     .label("jetstream::stream_root");
        let root_stream_sender = Sender::new_unlinked(selective_broadcast::full_broadcast);
        WorkerBuilder {
            persistence_backend,
            inner,
            flavor,
            snapshot_timer: Box::new(|| false),
            root_stream_sender,
        }
    }
}

impl<F, P> WorkerBuilder<F, P>
where
    F: RuntimeFlavor,
    P: PersistenceBackend,
{
    pub fn with_persistence_backend(mut self, backend: P) -> Self {
        self.persistence_backend = Rc::new(backend);
        self
    }

    pub fn with_snapshot_timer(mut self, timer: impl FnMut() -> bool + 'static) -> Self {
        self.snapshot_timer = Box::new(timer);
        self
    }

    pub fn new_stream(&mut self) -> JetStreamBuilder<NoKey, NoData, NoTime> {
        let mut op = pass_through_operator();
        // link our new stream to the root stream we will build later
        // so it can receive system messages
        selective_broadcast::link(&mut self.root_stream_sender, op.get_input_mut());
        JetStreamBuilder::from_operator(pass_through_operator(), self.inner.clone())
    }

    pub fn build(mut self) -> Result<Worker<<F as RuntimeFlavor>::Communication>, BuildError> {
        let mut snapshot_op =
            make_snapshot_controller(self.persistence_backend.clone(), self.snapshot_timer);
        snapshot_op.label("jetstream::snapshot".to_owned());

        // in `new_stream` we connected all streams to listen to `self.root_stream_sender`
        // now we swap these, so root_stream_sender is the output of snapshot_op
        // i.e. all streams will receive messages from snapshot_op
        std::mem::swap(&mut self.root_stream_sender, snapshot_op.get_output_mut());

        let ref_count = Rc::strong_count(&self.inner);
        let inner =
            Rc::try_unwrap(self.inner).map_err(|_| BuildError::UnfinishedStreams(ref_count - 1))?;

        let mut operators = inner.into_inner().unwrap().finish();
        operators.insert(0, Box::new(snapshot_op).into_buildable());

        let cluster_size = self.flavor.runtime_size();
        let this_worker = self.flavor.this_worker_id();
        let mut communication_backend = self.flavor.establish_communication()?;
        let operators: Vec<RunnableOperator> = operators
            .into_iter()
            .enumerate()
            .map(|(i, x)| {
                let label = x.get_label().unwrap_or(format!("operator_id_{}", i));
                let mut ctx = BuildContext::new(
                    this_worker,
                    i,
                    label,
                    self.persistence_backend.latest(this_worker),
                    &mut communication_backend,
                    0..cluster_size,
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

    pub(crate) fn add_stream<K: MaybeKey, V: MaybeData, T: MaybeTime>(
        &mut self,
        stream: JetStreamBuilder<K, V, T>,
    ) {
        // call void to destroy all remaining messages
        // TODO: With the current implementaion of inter-operator channels this is unnecessary
        // as they will drop messages if there are no receivers
        self.operators.extend(
            stream
                .void()
                .label("jetstream::stream_end")
                .into_operators(),
        )
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
    // this is the operator which reveives the union stream
    let mut unioned = pass_through_operator();

    let mut rt = runtime.lock().unwrap();
    for mut input_stream in streams {
        selective_broadcast::link(input_stream.get_output_mut(), unioned.get_input_mut());
        rt.add_stream(input_stream);
    }
    drop(rt);
    JetStreamBuilder::from_operator(unioned, runtime)
}

pub(crate) fn split_n<const N: usize, K: MaybeKey, V: MaybeData, T: MaybeTime>(
    runtime: Rc<Mutex<InnerRuntimeBuilder>>,
    input: JetStreamBuilder<K, V, T>,
    partitioner: impl OperatorPartitioner<K, V, T>,
) -> [JetStreamBuilder<K, V, T>; N] {
    let partition_op = OperatorBuilder::new_with_output_partitioning(
        |_| {
            |input, output, _ctx| {
                if let Some(x) = input.recv() {
                    output.send(x)
                }
            }
        },
        partitioner,
    );
    let mut input = input.then(partition_op);

    let new_streams: Vec<JetStreamBuilder<K, V, T>> = (0..N)
        .map(|_| {
            let mut operator = pass_through_operator();
            selective_broadcast::link(input.get_output_mut(), operator.get_input_mut());
            JetStreamBuilder::from_operator(operator, runtime.clone())
        })
        .collect();

    let mut rt = runtime.lock().unwrap();

    rt.add_stream(input);

    // SAFETY: We can unwrap because the vec was built from an iterator of size N
    // so the vec is guaranteed to fit
    unsafe { new_streams.try_into().unwrap_unchecked() }
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
        for op in self.operators.iter_mut().rev() {
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

#[cfg(test)]
mod tests {
    use crate::snapshot::NoPersistence;

    use crate::runtime::threaded::SingleThreadRuntime;
    use super::WorkerBuilder;

    /// check we can build the most basic runtime
    #[test]
    fn builds_basic_rt() {
        WorkerBuilder::new(SingleThreadRuntime)
            .build()
            .unwrap();
    }

    /// check we can build with persistance enabled
    #[test]
    fn builds_with_persistence() {
        let rt = WorkerBuilder::new(SingleThreadRuntime)
            .with_persistence_backend(NoPersistence::default())
            .with_snapshot_timer(|| false)
            .build()
            .unwrap();
    }
}
