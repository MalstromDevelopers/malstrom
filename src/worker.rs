use crate::channels::selective_broadcast;
use crate::frontier::{FrontierHandle, Probe, Timestamp};
use crate::snapshot::controller::{start_snapshot_region, RegionHandle, end_snapshot_region};
use crate::snapshot::{PersistenceBackend, SnapshotController};
use crate::stream::jetstream::{Data, JetStream, JetStreamBuilder, NoData};
use crate::stream::operator::{
    pass_through_operator, FrontieredOperator, RuntimeFrontieredOperator, StandardOperator,
};
pub struct Worker<P> {
    operators: Vec<FrontieredOperator<P>>,
    probes: Vec<Probe>,
    snapshot_handle: RegionHandle<P>
}
impl<P> Worker<P>
where
    P: PersistenceBackend
{
    pub fn new(snapshot_timer: impl FnMut() -> bool + 'static) -> (Worker<P>, JetStreamBuilder<NoData, P>) {
        let (new_stream, region_handle) = start_snapshot_region(snapshot_timer);
        let worker = Worker {
            operators: Vec::new(),
            probes: Vec::new(),
            snapshot_handle: region_handle
        };
        (worker, new_stream)
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

    pub fn get_frontier(&self) -> Option<Timestamp> {
        self.probes.last().map(|x| x.read())
    }

    pub fn step(&mut self) {
        for (i, op) in self.operators.iter_mut().enumerate().rev() {
            op.step(i);
            while op.has_queued_work() {
                op.step(i);
            }
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
            |input, output, frontier: &mut FrontierHandle, _| {
                let _ = frontier.advance_to(Timestamp::MAX);
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
}

// #[cfg(test)]
// mod test {
//     use crate::{
//         channels::selective_broadcast::{Receiver, Sender},
//         frontier::FrontierHandle,
//         stream::jetstream::JetStreamEmpty,
//         worker::Worker,
//     };

//     // Note this useful idiom: importing names from outer (for mod tests) scope.
//     use super::*;
//     use pretty_assertions::assert_eq;

//     #[test]
//     fn test_split() {
//         let mut worker = Worker::new();
//         let mut src_count = 0;
//         let source = move |_: &mut FrontierHandle| {
//             src_count += 1;
//             Some(src_count)
//         };

//         let stream = JetStreamEmpty::new().source(source);

//         let [evens, odds] =
//             worker.split_n::<2, _>(
//                 stream,
//                 |x: &i32, _out_n| if (x & 1) == 0 { vec![0] } else { vec![1] },
//             );

//         // Finalize all streams to step the workers
//         // let final_stream = stream.finalize(); // is already finalized in th split function

//         let (tx_a, rx_a) = crossbeam::channel::unbounded();
//         let (tx_b, rx_b) = crossbeam::channel::unbounded();

//         // Attach a sink to both operators, so we can observe the values
//         let evens = evens.then(StandardOperator::from(
//             move |input: &mut Receiver<i32>, _output: &mut Sender<i32>| {
//                 if let Some(msg) = input.recv() {
//                     tx_a.send(msg).unwrap()
//                 }
//             },
//         ));
//         let odds = odds.then(StandardOperator::from(
//             move |input: &mut Receiver<i32>, _output: &mut Sender<i32>| {
//                 if let Some(msg) = input.recv() {
//                     tx_b.send(msg).unwrap()
//                 }
//             },
//         ));

//         worker.add_stream(evens.build());
//         worker.add_stream(odds.build());

//         // Step a few times until the results trickle through
//         for _ in 0..5 {
//             worker.step();
//         }
//         assert_eq!(rx_a.try_recv().unwrap(), 2);
//         assert_eq!(rx_b.try_recv().unwrap(), 1);
//     }
// }
