use crate::channels::operator_io::{link, Input, Output};
use crate::stream::{AppendableOperator, OperatorBuilder, StreamBuilder};
use crate::types::{DataMessage, MaybeData, MaybeKey, MaybeTime};
use std::rc::Rc;

/// Split one datastream into multiple streams
pub trait Split<K, V, T>: super::sealed::Sealed {
    /// Split a stream into const N streams.
    /// Messages will be distributed according to the given partitioning function,
    /// the function receives a mutable array of booleans, all `false` by default,
    /// and should set all values in the array to `true` where output streams should
    /// receive a message. For example if you do a `const_split::<2>` and mutate the array to
    /// `[true, false]` the left output will receive the message.
    ///
    /// If you always want all outputs to receive every message
    /// see [crate::operators::Cloned::const_cloned].
    fn const_split<const N: usize>(
        self,
        name: &str,
        partitioner: impl Fn(&DataMessage<K, V, T>, &mut [bool; N]) + 'static,
    ) -> [StreamBuilder<K, V, T>; N];

    /// Split a stream into multiple streams
    /// Messages will be distributed according to the given partitioning function,
    /// the function receives a mutable array of booleans, all `false` by default,
    /// and should set all values in the array to `true` where output streams should
    /// receive a message. For example if you do a `const_split::<2>` and mutate the array to
    /// `[true, false]` the left output will receive the message.
    ///
    /// If you always want all outputs to receive every message
    /// see [crate::operators::Cloned::cloned].
    fn split(
        self,
        name: &str,
        partitioner: impl Fn(&DataMessage<K, V, T>, &mut [bool]) + 'static,
        outputs: usize,
    ) -> Vec<StreamBuilder<K, V, T>>;
}

impl<K, V, T> Split<K, V, T> for StreamBuilder<K, V, T>
where
    K: MaybeKey,
    V: MaybeData,
    T: MaybeTime,
{
    fn const_split<const N: usize>(
        self,
        name: &str,
        partitioner: impl Fn(&DataMessage<K, V, T>, &mut [bool; N]) + 'static,
    ) -> [StreamBuilder<K, V, T>; N] {
        let partitioner = move |msg: &DataMessage<K, V, T>, outputs: &mut [bool]| {
            // PANIC: Safe to unwrap as long as the impl of `split` is correct
            let outputs: &mut [bool; N] = outputs
                .try_into()
                .expect("Expected array size to match. This is a bug.");
            partitioner(msg, outputs)
        };
        let streams = self.split(name, partitioner, N);
        assert_eq!(streams.len(), N);
        // We need unwrap_unchecked because the stream builder does not implement Debug
        // SAFETY: We just asserted it fits
        unsafe { streams.try_into().unwrap_unchecked() }
    }

    fn split(
        self,
        name: &str,
        partitioner: impl Fn(&DataMessage<K, V, T>, &mut [bool]) + 'static,
        outputs: usize,
    ) -> Vec<StreamBuilder<K, V, T>> {
        let rt = self.get_runtime();
        let mut stream_receiver = self.finish_pop_tail();
        let mut downstream_receivers: Vec<Input<K, V, T>> =
            (0..outputs).map(|_| Input::new_unlinked()).collect();

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
            link(partition_op.get_output_mut(), dr);
        }
        #[allow(clippy::unwrap_used)]
        rt.lock()
            .unwrap()
            .add_operators([Box::new(partition_op).into_buildable()]);

        downstream_receivers
            .into_iter()
            .map(|x| StreamBuilder::from_receiver(x, Rc::clone(&rt)))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        operators::*,
        sinks::StatelessSink,
        sources::{SingleIteratorSource, StatelessSource},
        testing::{get_test_rt, VecSink},
    };

    /// Test const split
    #[test]
    fn const_split() {
        let even_sink = VecSink::new();
        let odd_sink = VecSink::new();

        let rt = get_test_rt(|provider| {
            let stream = provider.new_stream().source(
                "source",
                StatelessSource::new(SingleIteratorSource::new(0..10u64)),
            );
            let [even, odd] = stream.const_split("const-split", |msg, outputs| {
                let is_even = msg.value & 1 == 0;
                *outputs = [is_even, !is_even];
            });
            even.sink("sink-even", StatelessSink::new(even_sink.clone()));
            odd.sink("sink-odd", StatelessSink::new(odd_sink.clone()));
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

        let rt = get_test_rt(|provider| {
            let stream = provider.new_stream().source(
                "source",
                StatelessSource::new(SingleIteratorSource::new(0..10u64)),
            );
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
                2,
            );
            let odd = streams.pop().unwrap();
            let even = streams.pop().unwrap();
            even.sink("sink-even", StatelessSink::new(even_sink.clone()));
            odd.sink("sink-odd", StatelessSink::new(odd_sink.clone()));
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
