use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

use crate::channels::selective_broadcast::{self};
use crate::operators::void::Void;
use crate::snapshot::controller::make_snapshot_controller;
use crate::snapshot::PersistenceBackend;
use crate::stream::jetstream::JetStreamBuilder;
use crate::stream::operator::{
    pass_through_operator, BuildContext, BuildableOperator, OperatorBuilder, RunnableOperator,
};
use crate::time::{NoTime, Timestamp};
use crate::{Data, Key, NoData, NoKey, OperatorId, OperatorPartitioner, WorkerId};

pub struct Worker<P> {
    operators: Vec<Box<dyn BuildableOperator<P>>>,
    root_stream: JetStreamBuilder<NoKey, NoData, NoTime, P>,
}
impl<P> Worker<P>
where
    P: PersistenceBackend,
{
    pub fn new(snapshot_timer: impl FnMut() -> bool + 'static) -> Worker<P> {
        let snapshot_op = make_snapshot_controller(snapshot_timer);

        Worker {
            operators: Vec::new(),
            root_stream: JetStreamBuilder::from_operator(snapshot_op),
        }
    }

    pub fn new_stream(&mut self) -> JetStreamBuilder<NoKey, NoData, NoTime, P> {
        let mut new_op = pass_through_operator();
        selective_broadcast::link(self.root_stream.get_output_mut(), new_op.get_input_mut());
        JetStreamBuilder::from_operator(new_op)
    }

    pub fn add_stream<K: Key, V: Data, T: Timestamp>(
        &mut self,
        stream: JetStreamBuilder<K, V, T, P>,
    ) {
        // call void to destroy all remaining messages
        self.operators.extend(stream.void().into_operators())
    }

    /// Unions N streams with identical output types into a single stream
    pub fn union<K: Key, V: Data, T: Timestamp>(
        &mut self,
        streams: Vec<JetStreamBuilder<K, V, T, P>>,
    ) -> JetStreamBuilder<K, V, T, P> {
        // this is the operator which reveives the union stream
        let mut unioned = pass_through_operator();

        for mut input_stream in streams.into_iter() {
            selective_broadcast::link(input_stream.get_output_mut(), unioned.get_input_mut());
            self.add_stream(input_stream);
        }
        JetStreamBuilder::from_operator(unioned)
    }

    pub fn split_n<const N: usize, K: Key, V: Data, T: Timestamp>(
        &mut self,
        input: JetStreamBuilder<K, V, T, P>,
        partitioner: impl OperatorPartitioner<K, V, T>,
    ) -> [JetStreamBuilder<K, V, T, P>; N] {
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

        let new_streams: Vec<JetStreamBuilder<K, V, T, P>> = (0..N)
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
    ) -> Result<RuntimeWorker, postbox::BuildError> {
        // TODO: Add a `void` sink at the end of every dataflow to swallow
        // unused messages

        // TODO: make all of this configurable
        let listen_addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), config.port));
        let peers = config.get_peer_uris();

        let operator_ids: Vec<OperatorId> =
            (0..(self.operators.len() + self.root_stream.operator_count())).collect();
        let communication_backend =
            postbox::BackendBuilder::new(config.worker_id, listen_addr, peers, operator_ids, 128);

        let operators: Vec<RunnableOperator> = self
            .root_stream
            .into_operators()
            .into_iter()
            .chain(self.operators)
            .enumerate()
            .map(|(i, x)| {
                x.into_runnable(BuildContext::new(
                    config.worker_id,
                    i,
                    P::new_latest(config.worker_id),
                    communication_backend.for_operator(&i).unwrap(),
                ))
            })
            .collect();
        Ok(RuntimeWorker {
            worker_id: config.worker_id,
            operators,
            communication: communication_backend.connect()?,
        })
    }
}

pub struct RuntimeWorker {
    worker_id: WorkerId,
    operators: Vec<RunnableOperator>,
    communication: postbox::CommunicationBackend,
}
impl RuntimeWorker {
    // pub fn get_frontier(&self) -> Option<Timestamp> {
    //     self.probes.last().map(|x| x.read())
    // }

    // pub fn get_all_frontiers(&self) -> Vec<Timestamp> {
    //     self.probes.iter().map(|x| x.read()).collect()
    // }

    pub fn step(&mut self) {
        for op in self.operators.iter_mut().rev() {
            op.step();
            while op.has_queued_work() {
                op.step();
            }
        }
    }
}
