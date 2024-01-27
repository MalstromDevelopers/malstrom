use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

use crate::channels::selective_broadcast::{self};
use crate::frontier::{Probe, Timestamp};
use crate::snapshot::controller::{end_snapshot_region, start_snapshot_region, RegionHandle};
use crate::snapshot::PersistenceBackend;
use crate::stream::jetstream::{Data, JetStreamBuilder, NoData};
use crate::stream::operator::{
    pass_through_operator, FrontieredOperator, RunnableOperator, StandardOperator,
};

pub struct Worker<P> {
    operators: Vec<FrontieredOperator<P>>,
    probes: Vec<Probe>,
    root_stream: JetStreamBuilder<NoData, P>,
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
            probes: Vec::new(),
            root_stream: JetStreamBuilder::from_operator(snapshot_op),
            snapshot_handle: region_handle,
        }
    }

    pub fn new_stream(&mut self) -> JetStreamBuilder<NoData, P> {
        let mut new_op = pass_through_operator();
        selective_broadcast::link(self.root_stream.get_output_mut(), new_op.get_input_mut());
        JetStreamBuilder::from_operator(new_op)
    }

    pub fn add_stream<O: Data>(&mut self, stream: JetStreamBuilder<O, P>) {
        let stream = end_snapshot_region(stream, self.snapshot_handle.clone());
        for mut op in stream.build().into_operators().into_iter() {
            for p in self.probes.iter() {
                op.add_upstream_probe(p.clone())
            }
            self.probes.push(op.get_probe());
            self.operators.push(op);
        }
    }

    /// Unions N streams with identical output types into a single stream
    pub fn union<Output: Data>(
        &mut self,
        streams: Vec<JetStreamBuilder<Output, P>>,
    ) -> JetStreamBuilder<Output, P> {
        // this is the operator which reveives the union stream
        let mut unioned = pass_through_operator();

        for mut input_stream in streams.into_iter() {
            selective_broadcast::link(input_stream.get_output_mut(), unioned.get_input_mut());
            self.add_stream(input_stream);
        }
        JetStreamBuilder::from_operator(unioned)
    }

    pub fn split_n<const N: usize, Output: Data>(
        &mut self,
        input: JetStreamBuilder<Output, P>,
        partitioner: impl Fn(&Output, usize) -> Vec<usize> + 'static,
    ) -> [JetStreamBuilder<Output, P>; N] {
        let partition_op = StandardOperator::new_with_partitioning(
            |input, output, ctx| {
                ctx.frontier.advance_to(Timestamp::MAX);
                if let Some(x) = input.recv() {
                    output.send(x)
                }
            },
            partitioner,
        );
        let mut input = input.then(partition_op);

        let new_streams: Vec<JetStreamBuilder<Output, P>> = (0..N)
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

    pub fn build(self) -> Result<RuntimeWorker<P>, postbox::BuildError> {
        // TODO: make all of this configurable
        let listen_addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), 29091));
        let operator_ids = (0..self.operators.len())
            .map(|x| x.try_into().unwrap())
            .collect();
        // TODO: Get the peer addresses from K8S
        // should be "podname.sts-name"
        let peers = Vec::new();
        let communication_backend =
            postbox::BackendBuilder::new(listen_addr, peers, operator_ids, 128);

        let operators = self
            .root_stream
            .build()
            .into_operators()
            .into_iter()
            .chain(self.operators)
            .enumerate()
            .map(|(i, x)| {
                x.build(
                    i.try_into().unwrap(),
                    communication_backend
                        .for_operator(&i.try_into().unwrap())
                        .unwrap(),
                )
            })
            .collect();
        // NOTE: The root operator has no frontier and gets no probe
        Ok(RuntimeWorker {
            operators,
            probes: self.probes,
            communication: communication_backend.connect()?,
        })
    }
}

pub struct RuntimeWorker<P> {
    operators: Vec<RunnableOperator<P>>,
    probes: Vec<Probe>,
    communication: postbox::CommunicationBackend,
}
impl<P> RuntimeWorker<P>
where
    P: PersistenceBackend,
{
    pub fn get_frontier(&self) -> Option<Timestamp> {
        self.probes.last().map(|x| x.read())
    }

    pub fn step(&mut self) {
        for op in self.operators.iter_mut().rev() {
            op.step();
            while op.has_queued_work() {
                op.step();
            }
        }
    }
}
