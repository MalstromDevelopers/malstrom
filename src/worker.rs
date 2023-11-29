use crate::channels::selective_broadcast;
use crate::frontier::Probe;
use crate::stream::jetstream::{Data, JetStream, JetStreamBuilder};
use crate::stream::operator::{
    pass_through_operator, FrontieredOperator, RuntimeFrontieredOperator, StandardOperator,
};
pub struct Worker {
    operators: Vec<FrontieredOperator>,
    probes: Vec<Probe>,
}
impl Worker {
    pub fn new() -> Worker {
        Worker {
            operators: Vec::new(),
            probes: Vec::new(),
        }
    }

    pub fn add_stream(&mut self, stream: JetStream) {
        for mut op in stream.into_operators().into_iter() {
            for p in self.probes.iter() {
                op.add_upstream_probe(p.clone())
            }
            self.probes.push(op.get_probe());
            self.operators.push(op);
        }
    }

    pub fn get_frontier(&self) -> Option<u64> {
        self.probes.last().map(|x| x.read())
    }

    pub fn step(&mut self) {
        for (i, op) in self.operators.iter_mut().enumerate().rev() {
            op.step();
            while op.has_queued_work() {
                op.step();
            }
        }
    }

    /// Unions N streams with identical output types into a single stream
    pub fn union_n<const N: usize, Output: Data>(
        &mut self,
        streams: [JetStreamBuilder<Output>; N],
    ) -> JetStreamBuilder<Output> {
        // this is the operator which reveives the union stream
        let mut unioned = pass_through_operator();

        for mut input_stream in streams.into_iter() {
            selective_broadcast::link(input_stream.get_output_mut(), unioned.get_input_mut());
            self.add_stream(input_stream.build());
        }
        JetStreamBuilder::from_operator(unioned)
    }

    pub fn split_n<const N: usize, Output: Data>(
        &mut self,
        input: JetStreamBuilder<Output>,
        partitioner: impl Fn(&Output, usize) -> Vec<usize> + 'static,
    ) -> [JetStreamBuilder<Output>; N] {
        let partition_op = StandardOperator::new_with_partitioning(
            |input, output, _| {
                if let Some(msg) = input.recv() {
                    output.send(msg)
                }
            },
            partitioner,
        );
        let mut input = input.then(partition_op);

        let new_streams: Vec<JetStreamBuilder<Output>> = (0..N)
            .map(|_| {
                let mut operator = pass_through_operator();
                selective_broadcast::link(input.get_output_mut(), operator.get_input_mut());
                JetStreamBuilder::from_operator(operator)
            })
            .collect();

        self.add_stream(input.build());

        // SAFETY: We can unwrap because the vec was built from an iterator of size N
        // so the vec is guaranteed to fit
        unsafe { new_streams.try_into().unwrap_unchecked() }
    }
}

#[cfg(test)]
mod test {
    use crate::{
        channels::selective_broadcast::{Receiver, Sender},
        frontier::FrontierHandle,
        stream::jetstream::JetStreamEmpty,
        worker::Worker,
    };

    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn test_split() {
        let mut worker = Worker::new();
        let mut src_count = 0;
        let source = move |_: &mut FrontierHandle| {
            src_count += 1;
            Some(src_count)
        };

        let stream = JetStreamEmpty::new().source(source);

        let [evens, odds] =
            worker.split_n::<2, _>(
                stream,
                |x: &i32, _out_n| if (x & 1) == 0 { vec![0] } else { vec![1] },
            );

        // Finalize all streams to step the workers
        // let final_stream = stream.finalize(); // is already finalized in th split function

        let (tx_a, rx_a) = crossbeam::channel::unbounded();
        let (tx_b, rx_b) = crossbeam::channel::unbounded();

        // Attach a sink to both operators, so we can observe the values
        let evens = evens.then(StandardOperator::from(
            move |input: &mut Receiver<i32>, _output: &mut Sender<i32>| {
                if let Some(msg) = input.recv() {
                    tx_a.send(msg).unwrap()
                }
            },
        ));
        let odds = odds.then(StandardOperator::from(
            move |input: &mut Receiver<i32>, _output: &mut Sender<i32>| {
                if let Some(msg) = input.recv() {
                    tx_b.send(msg).unwrap()
                }
            },
        ));

        worker.add_stream(evens.build());
        worker.add_stream(odds.build());

        // Step a few times until the results trickle through
        for _ in 0..5 {
            worker.step();
        }
        assert_eq!(rx_a.try_recv().unwrap(), 2);
        assert_eq!(rx_b.try_recv().unwrap(), 1);
    }
}
