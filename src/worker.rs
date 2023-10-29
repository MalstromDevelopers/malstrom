use crossbeam::channel::{Receiver, Sender};

use crate::stream::{dist_rand, Data, DataUnion, JetStream, Operator, StandardOperator};

pub struct Worker {
    streams: Vec<Box<dyn Operator>>,
}
impl Worker {
    pub fn new() -> Worker {
        Worker {
            streams: Vec::new(),
        }
    }

    pub fn add_stream<I: Data, O: Data>(&mut self, stream: JetStream<I, O>) {
        self.streams.push(Box::new(stream));
    }

    pub fn step(&mut self) {
        for stream in self.streams.iter_mut().rev() {
            stream.step();
            if stream.has_input() {
                break;
            }
        }
    }

    /// union two streams
    pub fn union<Input: Data, Output: Data, InputB: Data, OutputB: Data>(
        &mut self,
        left: JetStream<Input, Output>,
        right: JetStream<InputB, OutputB>,
    ) -> JetStream<DataUnion<Output, OutputB>, DataUnion<Output, OutputB>> {
        let mut new_stream = JetStream::new();

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

        transform_left.add_output(new_stream.tail_mut());
        transform_right.add_output(new_stream.tail_mut());

        self.streams.push(Box::new(left.then(transform_left)));
        self.streams.push(Box::new(right.then(transform_right)));
        new_stream
    }
}
