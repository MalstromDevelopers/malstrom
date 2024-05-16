use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::rc::Rc;

use crate::channels::selective_broadcast::{self};
use crate::operators::void::Void;
use crate::snapshot::controller::make_snapshot_controller;
use crate::snapshot::{NoPersistence, PersistenceBackend, PersistenceBackendBuilder};
use crate::stream::jetstream::JetStreamBuilder;
use crate::stream::operator::{
    pass_through_operator, BuildContext, BuildableOperator, OperatorBuilder, RunnableOperator,
};
use crate::time::{MaybeTime, NoTime};
use crate::{MaybeData, MaybeKey, NoData, NoKey, OperatorId, OperatorPartitioner, WorkerId};

pub struct Worker {
    operators: Vec<Box<dyn BuildableOperator>>,
    root_stream: JetStreamBuilder<NoKey, NoData, NoTime>,
    persistence_backend: Rc<dyn PersistenceBackendBuilder>,
}
impl Worker {
    pub fn new(
        persistence_backend: impl PersistenceBackendBuilder,
        snapshot_timer: impl FnMut() -> bool + 'static,
    ) -> Worker {
        let persistence_backend = Rc::new(persistence_backend);
        let snapshot_op = make_snapshot_controller(persistence_backend.clone(), snapshot_timer);

        Worker {
            operators: Vec::new(),
            root_stream: JetStreamBuilder::from_operator(snapshot_op)
                .label("jetstream::stream_root"),
            persistence_backend,
        }
    }

    pub fn new_stream(&mut self) -> JetStreamBuilder<NoKey, NoData, NoTime> {
        let mut new_op = pass_through_operator();
        selective_broadcast::link(self.root_stream.get_output_mut(), new_op.get_input_mut());
        JetStreamBuilder::from_operator(new_op).label("jetstream::pass_through")
    }

    pub fn add_stream<K: MaybeKey, V: MaybeData, T: MaybeTime>(
        &mut self,
        stream: JetStreamBuilder<K, V, T>,
    ) {
        // call void to destroy all remaining messages
        self.operators.extend(
            stream
                .void()
                .label("jetstream::stream_end")
                .into_operators(),
        )
    }

    /// Unions N streams with identical output types into a single stream
    pub fn union<K: MaybeKey, V: MaybeData, T: MaybeTime>(
        &mut self,
        streams: Vec<JetStreamBuilder<K, V, T>>,
    ) -> JetStreamBuilder<K, V, T> {
        // this is the operator which reveives the union stream
        let mut unioned = pass_through_operator();

        for mut input_stream in streams.into_iter() {
            selective_broadcast::link(input_stream.get_output_mut(), unioned.get_input_mut());
            self.add_stream(input_stream);
        }
        JetStreamBuilder::from_operator(unioned)
    }

    pub fn split_n<const N: usize, K: MaybeKey, V: MaybeData, T: MaybeTime>(
        &mut self,
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
                JetStreamBuilder::from_operator(operator)
            })
            .collect();

        self.add_stream(input);

        // SAFETY: We can unwrap because the vec was built from an iterator of size N
        // so the vec is guaranteed to fit
        unsafe { new_streams.try_into().unwrap_unchecked() }
    }

    pub fn build(
        self,
        config: crate::config::Config,
    ) -> Result<Runtime, postbox::errors::BuildError> {
        // TODO: make all of this configurable
        let listen_addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), config.port));
        let peers = config.get_cluster_uris();
        let peer_len = peers.len();
        let operator_ids: Vec<OperatorId> =
            (0..(self.operators.len() + self.root_stream.operator_count())).collect();

        let mut communication_backend = postbox::PostboxBuilder::new()
            .build(listen_addr, move |addr: &(WorkerId, OperatorId)| {
                peers.get(&addr.0).map(|x| x.clone())
            })
            .unwrap();

        let operators: Vec<RunnableOperator> = self
            .root_stream
            .into_operators()
            .into_iter()
            .chain(self.operators)
            .enumerate()
            .map(|(i, x)| {
                let label = x.get_label().unwrap_or(format!("operator_id_{}", i));
                let mut ctx = BuildContext::new(
                    config.worker_id,
                    i,
                    label,
                    self.persistence_backend.latest(config.worker_id),
                    &mut communication_backend,
                    0..peer_len,
                );
                x.into_runnable(&mut ctx)
            })
            .collect();
        Ok(Runtime {
            worker_id: config.worker_id,
            operators,
            communication: communication_backend,
        })
    }
}

pub struct Runtime {
    worker_id: WorkerId,
    operators: Vec<RunnableOperator>,
    communication: postbox::Postbox<(WorkerId, OperatorId)>,
}
impl Runtime {
    pub fn step(&mut self) {
        let span = tracing::info_span!("scheduling::run_graph");
        let _span_guard = span.enter();
        for op in self.operators.iter_mut().rev() {
            op.step(&mut self.communication);
            while op.has_queued_work() {
                op.step(&mut self.communication);
            }
        }
    }
}
