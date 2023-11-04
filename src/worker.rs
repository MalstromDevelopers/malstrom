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

    /// split stream into n streams
    pub fn split<Output: Data>(
        &mut self,
        input: JetStream<Output>,
        mut partitioner: impl FnMut(&Output) -> usize + 'static, // TODO: Should this take in the partition count?
        partition_count: usize,
    ) -> Vec<JetStream<Output>> {
        let communication: Vec<_> = (0..partition_count).map(|_| unbounded()).collect();

        let mut distributer = StandardOperator::from(
            move |inputs: &Vec<Receiver<Output>>, outputs: &Vec<Sender<Output>>| {
                let data = inputs.iter().filter_map(|x| x.try_recv().ok());

                let out_len: usize = outputs.len();

                for d in data {
                    // calculate the output stream by moduloing the return of the partition function
                    let target_output_index = partitioner(&d) % out_len;

                    outputs[target_output_index]
                        .send(d)
                        .expect("Failed to send data downstream")
                }
            },
        );

        let output_operators: Vec<JetStream<Output>> = communication
            .into_iter()
            .map(|(tx, rx)| {
                distributer.add_output(tx);

                let mut op = StandardOperator::from(
                    |inputs: &Vec<Receiver<Output>>, outputs: &Vec<Sender<Output>>| {
                        let data = inputs.iter().filter_map(|x| x.try_recv().ok());
                        dist_rand(data, outputs)
                    },
                );
                op.add_input(rx);

                JetStream::from_operator(op)
            })
            .collect();

        self.add_stream(input.then(distributer).finalize());

        output_operators
    }
}

#[cfg(test)]
mod test {
    use crate::{frontier::FrontierHandle, worker::Worker};

    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn test_split() {
        println!("Build new stream");
        let mut worker = Worker::new();
        println!("Add source");
        let source = |_: &mut FrontierHandle| Some(rand::random::<f64>().clamp(0.0, 1.0));

        let stream = JetStream::new().source(source);

        println!("Split stream");
        let split_streams = worker.split(stream, |x: &f64| if *x < 0.5 { 0 } else { 1 }, 2);

        println!("Finalize streams");
        // Finalize all streams to step the workers
        // let final_stream = stream.finalize(); // is already finalized in th split function
        let final_split_stream: Vec<JetStream<Finalized>> =
            split_streams.into_iter().map(|o| o.finalize()).collect();

        assert_eq!(final_split_stream.len(), 2);

        println!("Step twice");
        // TODO: This results in an infinite loop???
        // worker.step();
        // worker.step();

        assert_eq!(
            final_split_stream
                .into_iter()
                .map(|o| o.get_probe().read())
                .sum::<u64>(),
            2
        );
    }
}
