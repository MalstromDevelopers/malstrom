use crate::channels::operator_io::{Input, Output, link};
use crate::stream::IntoBuildable;
use crate::stream::{Operator, SafeLogic, StreamBuilder};
use crate::types::{DataMessage, Kvt, MaybeData, MaybeKey, MaybeTime, Message, Sealed};
use std::marker::PhantomData;
use std::rc::Rc;

/// Split one datastream into multiple streams
pub trait Split<Msg: Kvt>: Sealed {
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
        name: impl Into<String>,
        partitioner: impl Fn(&DataMessage<Msg>, &mut [bool; N]) + 'static,
    ) -> [StreamBuilder<Msg>; N];

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
        name: impl Into<String>,
        partitioner: impl Fn(&DataMessage<Msg>, &mut [bool]) + 'static,
        outputs: usize,
    ) -> Vec<StreamBuilder<Msg>>;
}

impl<Msg> Split<Msg> for StreamBuilder<Msg>
where
    Msg: Kvt,
    Msg::Key: MaybeKey,
    Msg::Value: MaybeData,
    Msg::Timestamp: MaybeTime,
{
    fn const_split<const N: usize>(
        self,
        name: impl Into<String>,
        partitioner: impl Fn(&DataMessage<Msg>, &mut [bool; N]) + 'static,
    ) -> [StreamBuilder<Msg>; N] {
        let partitioner = move |msg: &DataMessage<Msg>, outputs: &mut [bool]| {
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
        name: impl Into<String>,
        partitioner: impl Fn(&DataMessage<Msg>, &mut [bool]) + 'static,
        outputs: usize,
    ) -> Vec<StreamBuilder<Msg>> {
        let rt = self.get_runtime();
        let mut input = self.tail;
        let output = Output::new_unlinked(partitioner);

        let mut downstream_receivers: Vec<Input<Msg>> =
            (0..outputs).map(|_| Input::new_unlinked()).collect();

        let mut partition_op =
            Operator::direct(name.into(), Forward(PhantomData::<Msg>).into_logic());
        // we perform a swap so our new operator will get the messages
        // which come out of the input stream
        std::mem::swap(&mut partition_op.input, &mut input);

        // link all downstream receivers to our partition op
        for dr in downstream_receivers.iter_mut() {
            link(&mut partition_op.output, dr);
        }
        #[allow(clippy::unwrap_used)]
        rt.lock()
            .unwrap()
            .add_operator(Box::new(partition_op.into_buildable()));

        downstream_receivers
            .into_iter()
            .map(|x| StreamBuilder {
                tail: x,
                runtime: Rc::clone(&rt),
            })
            .collect()
    }
}

struct Forward<Msg>(PhantomData<Msg>);
impl<Msg> SafeLogic<Msg, Msg> for Forward<Msg>
where
    Msg: Kvt,
{
    fn on_data(
        &mut self,
        data_message: DataMessage<Msg>,
        output: &mut Output<Msg>,
        ctx: &mut crate::stream::OperatorContext,
    ) {
        output.send(Message::Data(data_message));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        operators::*,
        sinks::StatelessSink,
        sources::{SingleIteratorSource, StatelessSource},
        testing::{VecSink, get_test_rt},
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
