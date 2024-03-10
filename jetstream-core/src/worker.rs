use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

use crate::channels::selective_broadcast::{self};
use crate::snapshot::controller::{end_snapshot_region, start_snapshot_region, RegionHandle};
use crate::snapshot::PersistenceBackend;
use crate::stream::jetstream::JetStreamBuilder;
use crate::stream::operator::{
    pass_through_operator, FrontieredOperator, RunnableOperator, StandardOperator,
};
use crate::time::{NoTime, Timestamp};
use crate::{Data, Key, NoData, NoKey, OperatorId, OperatorPartitioner, WorkerId};

pub struct Worker<P> {
    operators: Vec<FrontieredOperator>,
    root_stream: JetStreamBuilder<NoKey, NoData, NoTime, P>,
    snapshot_handle: RegionHandle<P>,
}
impl<P> Worker<P>
where
    P: PersistenceBackend,
{
    pub fn new(snapshot_timer: impl FnMut() -> bool + 'static) -> Worker<P> {
        let (snapshot_op, region_handle) = start_snapshot_region(snapshot_timer);

        Worker {
            operators: Vec::new(),
            root_stream: JetStreamBuilder::from_operator(snapshot_op),
            snapshot_handle: region_handle,
        }
    }

    pub fn new_stream(&mut self) -> JetStreamBuilder<NoKey, NoData, NoTime, P> {
        let mut new_op = pass_through_operator();
        selective_broadcast::link(self.root_stream.get_output_mut(), new_op.get_input_mut());
        JetStreamBuilder::from_operator(new_op)
    }

    pub fn add_stream<K: Key, V: Data, T: Timestamp>(&mut self, stream: JetStreamBuilder<K, V, T, P>) {
        let stream = end_snapshot_region(stream, self.snapshot_handle.clone());
        self.operators.extend(stream.build().into_operators())
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
        let partition_op = StandardOperator::new_with_output_partitioning(
            |input, output, _ctx| {
                if let Some(x) = input.recv() {
                    output.send(x)
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

    pub fn build(self, config: Option<crate::config::Config>) -> Result<RuntimeWorker, postbox::BuildError> {
        // TODO: Add a `void` sink at the end of every dataflow to swallow
        // unused messages
        let config = config.as_ref().unwrap_or(&crate::config::CONFIG);

        // TODO: make all of this configurable
        let listen_addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), config.port));
        let _operator_ids: Vec<OperatorId> = (0..self.operators.len())
            .collect();
        // TODO: Get the peer addresses from K8S
        // should be "podname.sts-name"
        let peers = config.get_peer_uris();
        let operators: Vec<(usize, FrontieredOperator)> = self
            .root_stream
            .build()
            .into_operators()
            .into_iter()
            .chain(self.operators)
            .enumerate()
            .collect();
        let operator_ids = operators.iter().map(|(i, _)| *i).collect();
        let communication_backend =
            postbox::BackendBuilder::new(config.worker_id, listen_addr, peers, operator_ids, 128);
        let operators = operators.into_iter()
            .map(|(i, x)| {
                x.build(
                    config.worker_id,
                    i,
                    communication_backend
                        .for_operator(&i)
                        .unwrap(),
                )
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
impl RuntimeWorker
{
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
