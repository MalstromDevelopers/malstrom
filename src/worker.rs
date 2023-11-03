use crossbeam::channel::{Receiver, Sender, unbounded};

use crate::{stream::{dist_rand, Data, DataUnion, JetStream, StandardOperator, Finalized, AddOutput, AddInput}, frontier::{Frontier, FrontierHandle, Probe}, watch::channel};

pub struct Worker {
    streams: Vec<JetStream<Finalized>>,
    probes: Vec<Probe>
}
impl Worker {
    pub fn new() -> Worker {
        Worker {
            streams: Vec::new(),
            probes: Vec::new()
        }
    }

    pub fn add_stream(&mut self, stream: JetStream<Finalized>) {
        self.probes.push(stream.get_probe());
        self.streams.push(stream);
    }

    pub fn get_frontier(&self) -> Option<u64> {
        self.probes.last().and_then(|x| Some(x.read()))
    }

    pub fn step(&mut self) {
        for (i, stream) in self.streams.iter_mut().enumerate().rev() {
            let upstream_frontiers = self.probes[..i].iter().map(|x| x.read()).collect();
            stream.step(&upstream_frontiers);
            while stream.has_queued_work() {
                stream.step(&upstream_frontiers);
            }
        }
    }

    /// union two streams
    pub fn union<Output: Data, OutputB: Data>(
        &mut self,
        left: JetStream<Output>,
        right: JetStream<OutputB>,
    ) -> JetStream<DataUnion<Output, OutputB>> {
        // wrap data from left into a data union
        let mut transform_left = StandardOperator::from(
            |inputs: &Vec<Receiver<Output>>, outputs: &Vec<Sender<DataUnion<Output, OutputB>>>| {
                let data = inputs
                    .iter()
                    .map(|x| x.try_recv().ok())
                    .filter_map(|x| x.map(|y| DataUnion::Left(y)));
                dist_rand(data, outputs)
            },
        );
        // wrap data from right into a data union
        let mut transform_right = StandardOperator::from(
            |inputs: &Vec<Receiver<OutputB>>, outputs: &Vec<Sender<DataUnion<Output, OutputB>>>| {
                let data = inputs
                    .iter()
                    .map(|x| x.try_recv().ok())
                    .filter_map(|x| x.map(|y| DataUnion::Right(y)));
                dist_rand(data, outputs)
            },
        );

        let mut unioned = StandardOperator::from(
            |inputs: &Vec<Receiver<DataUnion<Output, OutputB>>>, outputs: &Vec<Sender<DataUnion<Output, OutputB>>>| {
                let data = inputs
                    .iter()
                    .map(|x| x.try_recv().ok())
                    .filter_map(|x| x);
                dist_rand(data, outputs)
            },
        );

        let (tx1, rx1) = unbounded();
        let (tx2, rx2) = unbounded();
        transform_left.add_output(tx1);
        transform_right.add_output(tx2);
        unioned.add_input(rx1);
        unioned.add_input(rx2);

        self.add_stream(left.then(transform_left).finalize());
        self.add_stream(right.then(transform_right).finalize());

        JetStream::from_operator(unioned)
    }
}
