use crate::channels::selective_broadcast::{link, Input};
use crate::stream::{AppendableOperator, JetStreamBuilder, OperatorBuilder};
use crate::types::{MaybeData, MaybeKey, MaybeTime, OperatorPartitioner};
use std::rc::Rc;

pub trait Split<K, V, T> {
    /// Split a stream into N streams.
    /// Messages will be distributed according to the given partitioning function.
    ///
    /// # Example
    /// ```
    /// use jetstream::operators::*;
    /// use jetstream::runtime::{WorkerBuilder, threaded::SingleThreadRuntimeFlavor};
    /// use jetstream::testing::VecSink;
    /// use jetstream::sources::SingleIteratorSource;
    ///
    /// let even_sink = VecSink::new();
    /// let odd_sink = VecSink::new();
    ///
    /// let rt = SingleThreadRuntime::new(|flavor| {
    ///     let mut worker = WorkerBuilder::new(flavor, || false, NoPersistence::default());
    ///     let stream = worker.new_stream().source(SingleIteratorSource::new(0..10u64));
    ///     let [even, odd] = stream.split_n(|msg, i| vec![msg.value % i]);
    ///     even.sink(even_sink.clone()).finish();
    ///     odd.sink(odd_sink.clone()).finish();
    ///     worker
    /// });
    /// rt.execute().unwrap();
    ///
    /// let even_expected = vec![0, 2, 4, 6, 8];
    /// let even_result: Vec<u64> = even_sink.into_iter().map(|x| x.value).collect();
    /// assert_eq!(even_expected, even_result);
    ///
    /// let odd_expected = vec![1, 3, 5, 7, 9];
    /// let odd_result: Vec<u64> = odd_sink.into_iter().map(|x| x.value).collect();
    /// assert_eq!(odd_expected, odd_result);
    /// ```
    fn split_n<const N: usize>(
        self,
        partitioner: impl OperatorPartitioner<K, V, T>,
    ) -> [JetStreamBuilder<K, V, T>; N];
}

impl<K, V, T> Split<K, V, T> for JetStreamBuilder<K, V, T>
where
    K: MaybeKey,
    V: MaybeData,
    T: MaybeTime,
{
    fn split_n<const N: usize>(
        self,
        partitioner: impl OperatorPartitioner<K, V, T>,
    ) -> [JetStreamBuilder<K, V, T>; N] {
        let rt = self.get_runtime();
        let mut stream_receiver = self.finish_pop_tail();
        let mut downstream_receivers: [Input<K, V, T>; N] =
            core::array::from_fn(|_| Input::new_unlinked());

        let mut partition_op = OperatorBuilder::new_with_output_partitioning(
            |_| {
                |input, output, _ctx| {
                    if let Some(x) = input.recv() {
                        output.send(x)
                    }
                }
            },
            partitioner,
        );
        // we perform a swap so our new operator will get the messages
        // which come out of the input stream
        std::mem::swap(partition_op.get_input_mut(), &mut stream_receiver);

        // link all downstream receivers to our partition op
        for dr in downstream_receivers.iter_mut() {
            link(partition_op.get_output_mut(), dr);
        }
        rt.lock()
            .unwrap()
            .add_operators([Box::new(partition_op).into_buildable()]);

        downstream_receivers.map(|x| JetStreamBuilder::from_receiver(x, Rc::clone(&rt)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        prelude::{SingleIteratorSource, Sink, Source, WorkerBuilder},
        runtime::threaded::SingleThreadRuntime,
        snapshot::NoPersistence,
        testing::VecSink,
    };

    /// Test const split
    #[test]
    fn const_split() {
        let even_sink = VecSink::new();
        let odd_sink = VecSink::new();

        let rt = SingleThreadRuntime::new(|flavor| {
            let mut worker = WorkerBuilder::new(flavor, || false, NoPersistence::default());
            let stream = worker
                .new_stream()
                .source(SingleIteratorSource::new(0..10u64));
            let [even, odd] = stream.split_n(|msg, i| vec![msg.value % i]);
            even.sink(even_sink.clone()).finish();
            odd.sink(odd_sink.clone()).finish();
            worker
        });
        rt.execute().unwrap();

        let even_expected = vec![0, 2, 4, 6, 8];
        let even_result: Vec<u64> = even_sink.into_iter().map(|x| x.value).collect();
        assert_eq!(even_expected, even_result);

        let odd_expected = vec![1, 3, 5, 7, 9];
        let odd_result: Vec<u64> = odd_sink.into_iter().map(|x| x.value).collect();
        assert_eq!(odd_expected, odd_result);
    }
}
