use std::iter;

use crate::{
    channels::selective_broadcast::{Receiver, Sender},
    operators::IntoSource,
    stream::OperatorBuilder,
    types::{Data, DataMessage, Message, NoData, NoKey, NoTime},
};

/// A datasource which yields values from an iterator
/// on **one single worker**.
/// In the current implementation, values are always emitted
/// on worker `0`.
///
/// Emitted values are timestamped with their index in the iterator.
/// This sink emits an epoch `usize::MAX` after the last
/// element in the iterator.
///
/// # Example
/// ```rust
/// use jetstream::operators::*;
/// use jetstream::operators::Source;
/// use jetstream::runtime::{WorkerBuilder, threaded::SingleThreadRuntimeFlavor};
/// use jetstream::testing::VecSink;
/// use jetstream::sources::SingleIteratorSource;
///
/// let sink = VecSink::new();
/// let mut worker = WorkerBuilder::new(SingleThreadRuntimeFlavor::default());
///
/// worker
///     .new_stream()
///     .source(SingleIteratorSource::new(0..100))
///     .sink(sink.clone())
///     .finish();
///
/// worker.build().expect("can build").execute();
/// let expected: Vec<i32> = (0..100).collect();
/// let out: Vec<i32> = sink.into_iter().map(|x| x.value).collect();
/// assert_eq!(out, expected);
/// ```
pub struct SingleIteratorSource<T> {
    iterator: Box<dyn Iterator<Item = T>>,
}

impl<T> SingleIteratorSource<T> {
    pub fn new<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = T>,
        <I as IntoIterator>::IntoIter: 'static,
    {
        Self {
            iterator: Box::new(iter.into_iter()),
        }
    }
}

impl<V> IntoSource<NoKey, V, usize> for SingleIteratorSource<V>
where
    V: Data,
{
    fn into_source(self) -> OperatorBuilder<NoKey, NoData, NoTime, NoKey, V, usize> {
        OperatorBuilder::built_by(|build_context| {
            let mut inner = if build_context.worker_id == 0 {
                self.iterator.enumerate()
            } else {
                // do not emit on non-0 worker
                (Box::new(iter::empty::<V>()) as Box<dyn Iterator<Item = V>>).enumerate()
            };
            let mut final_emitted = false;

            move |input: &mut Receiver<NoKey, NoData, NoTime>,
                  output: &mut Sender<NoKey, V, usize>,
                  ctx| {
                if !final_emitted {
                    if let Some(x) = inner.next() {
                        output.send(Message::Data(DataMessage::new(NoKey, x.1, x.0)));
                    } else {
                        output.send(Message::Epoch(usize::MAX));
                        final_emitted = true;
                    }
                }
                if let Some(msg) = input.recv() {
                    match msg {
                        Message::Data(_) => (),
                        Message::Epoch(_) => (),
                        Message::AbsBarrier(x) => output.send(Message::AbsBarrier(x)),
                        // Message::Load(x) => output.send(Message::Load(x)),
                        Message::Rescale(x) => output.send(Message::Rescale(x)),
                        Message::SuspendMarker(x) => output.send(Message::SuspendMarker(x)),
                        Message::Interrogate(x) => output.send(Message::Interrogate(x)),
                        Message::Collect(x) => output.send(Message::Collect(x)),
                        Message::Acquire(x) => output.send(Message::Acquire(x)),
                    }
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {

    use itertools::Itertools;
    use proptest::bits::usize;

    use crate::{
        operators::*,
        runtime::{threaded::MultiThreadRuntime, WorkerBuilder},
        sources::SingleIteratorSource,
        testing::{get_test_stream, VecSink},
        types::Message,
    };

    /// The into_iter source should emit the iterator values
    #[test]
    fn emits_values() {
        let (builder, stream) = get_test_stream();

        let in_data: Vec<i32> = (0..100).collect();
        let collector = VecSink::new();

        let stream = stream
            .source(SingleIteratorSource::new(in_data))
            .sink(collector.clone());

        stream.finish();
        builder.build().unwrap().execute();

        let c = collector.into_iter().map(|x| x.value).collect_vec();
        assert_eq!(c, (0..100).collect_vec())
    }

    /// It should only emit records on worker 0 to avoid duplicates
    #[test]
    fn emits_only_on_worker_0() {
        let sink = VecSink::new();

        let args = [sink.clone(), sink.clone()];

        let rt = MultiThreadRuntime::new_with_args(
            |flavor, this_sink| {
                let mut builder = WorkerBuilder::new(flavor);
                builder
                    .new_stream()
                    .source(SingleIteratorSource::new(0..10))
                    .sink(this_sink)
                    .finish();
                builder
            },
            args,
        );

        rt.execute().unwrap();
        // if both threads emitted values, we would expect this to contain 20 values, not 10
        assert_eq!(sink.len(), 10);
    }

    /// values should be timestamped with their iterator index
    #[test]
    fn emits_timestamped_messages() {
        let (builder, stream) = get_test_stream();
        let sink = VecSink::new();
        stream
            .source(SingleIteratorSource::new(42..52))
            .sink(sink.clone())
            .finish();
        builder.build().unwrap().execute();

        let timestamps = sink.into_iter().map(|x| x.timestamp).collect_vec();
        let expected = (0..10).collect_vec();
        assert_eq!(expected, timestamps);
    }

    /// after the final value a single usize::MAX epoch should
    /// be emitted
    #[test]
    fn emits_max_epoch() {
        let (builder, stream) = get_test_stream();
        let sink = VecSink::new();
        stream
            .source(SingleIteratorSource::new(0..10))
            .sink_full(sink.clone())
            .finish();
        builder.build().unwrap().execute();

        let messages = sink.drain_vec(..);
        let last = messages.last().unwrap();
        match last {
            Message::Epoch(usize::MAX) => (),
            _ => panic!(),
        }
    }
}
