use crate::channels::operator_io::{link, Input, Output};
use crate::stream::{AppendableOperator, JetStreamBuilder, OperatorBuilder};
use crate::types::{DataMessage, MaybeData, MaybeKey, MaybeTime, OperatorPartitioner};
use std::rc::Rc;

pub trait Split<K, V, T>: super::sealed::Sealed {
    /// Split a stream into const N streams.
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
    fn const_split<const N: usize>(
        self,
        name: &str,
        partitioner: impl Fn(&DataMessage<K, V, T>, &mut [bool; N]) -> () + 'static,
    ) -> [JetStreamBuilder<K, V, T>; N];

    /// Split a stream into multiple streams
    fn split( self, 
        name: &str,
        partitioner: impl Fn(&DataMessage<K, V, T>, &mut [bool]) -> () + 'static, outputs: usize ) -> Vec<JetStreamBuilder<K, V, T>>;
}

impl<K, V, T> Split<K, V, T> for JetStreamBuilder<K, V, T>
where
    K: MaybeKey,
    V: MaybeData,
    T: MaybeTime,
{
    fn const_split<const N: usize>(
        self,
        name: &str,
        partitioner: impl Fn(&DataMessage<K, V, T>, &mut [bool; N]) -> () + 'static,
    ) -> [JetStreamBuilder<K, V, T>; N] {
        let partitioner = move |msg: &DataMessage<K, V, T>, outputs: &mut [bool]| {
            // PANIC: Safe to unwrap as long as the impl of `split` is correct
            let outputs: &mut [bool; N] = outputs.try_into().expect(
                "Expected array size to match. This is a bug."
            );
            partitioner(msg, outputs)
        };
        let streams = self.split(name, partitioner, N);
        assert_eq!(streams.len(), N);
        // We need unwrap_unchecked because the stream builder does not implement Debug
        // SAFETY: We just asserted it fits
        unsafe {streams.try_into().unwrap_unchecked()}
    }

    fn split(self, name:&str, partitioner: impl Fn(&DataMessage<K, V, T>, &mut [bool]) -> () + 'static, outputs: usize ) -> Vec<JetStreamBuilder<K, V, T>> {
        let rt = self.get_runtime();
        let mut stream_receiver = self.finish_pop_tail();
        let mut downstream_receivers: Vec<Input<K, V, T>>  = (0..outputs).map(|_| Input::new_unlinked()).collect();

        let output = Output::new_unlinked(partitioner);

        let mut partition_op = OperatorBuilder::new_with_output(
            name,
            |_| {
                |input, output, _ctx| {
                    if let Some(x) = input.recv() {
                        output.send(x)
                    }
                }
            },
            output,
        );
        // we perform a swap so our new operator will get the messages
        // which come out of the input stream
        std::mem::swap(partition_op.get_input_mut(), &mut stream_receiver);

        // link all downstream receivers to our partition op
        for dr in downstream_receivers.iter_mut() {
            link(partition_op.get_output_mut(),  dr);
        }
        rt.lock()
            .unwrap()
            .add_operators([Box::new(partition_op).into_buildable()]);

        downstream_receivers.into_iter().map(|x| JetStreamBuilder::from_receiver(x, Rc::clone(&rt))).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        operators::*,
        runtime::{threaded::SingleThreadRuntime, WorkerBuilder}, snapshot::NoPersistence, sources::SingleIteratorSource, testing::VecSink
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
                .source("source", SingleIteratorSource::new(0..10u64));
            let [even, odd] = stream.const_split(
                "const-split",
                |msg, outputs| {
                    let is_even = msg.value & 1 == 0;
                    *outputs = [is_even, !is_even];
                }
            
            );
            even.sink("sink-even", even_sink.clone()).finish();
            odd.sink("sink-odd", odd_sink.clone()).finish();
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

    /// Test non-const split
    #[test]
    fn split() {
        let even_sink = VecSink::new();
        let odd_sink = VecSink::new();

        let rt = SingleThreadRuntime::new(|flavor| {
            let mut worker = WorkerBuilder::new(flavor, || false, NoPersistence::default());
            let stream = worker
                .new_stream()
                .source("source", SingleIteratorSource::new(0..10u64));
            let mut streams = stream.split(
                "split", 
                |msg, outputs| {
                    if msg.value & 1 == 0 {
                        // even
                        outputs[0] = true;
                    } else {
                        outputs[1] = true;
                    }
                }, 
                2);
            let odd = streams.pop().unwrap();
            let even = streams.pop().unwrap();
            even.sink("sink-even", even_sink.clone()).finish();
            odd.sink("sink-odd", odd_sink.clone()).finish();
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
