use std::iter;

use crate::{
    channels::selective_broadcast::{Receiver, Sender}, operators::IntoSource, stream::{Logic, OperatorBuilder}, types::{Data, DataMessage, Message, NoData, NoKey, NoTime
}};

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
/// ```
/// # use jetstream::sources::SingleIteratorSource;
/// use jetstream::{JetStreamBuilder, RuntimeBuilder};
/// use jetstream::test::TestingValuesSink;
/// use jetstream::runtimes::SingleThreadRuntime;
///
/// let values = vec!["foo", "bar", "baz"];
/// let output = Vec::new();
/// let rt = RuntimeBuilder::new(SingleThreadRuntime::default())
/// rt.new_stream()
///     .source(SingleIteratorSource::new(values))
///     .sink(TestingValuesSink::new(&mut output)); // collects into output
///
/// rt.build().execute() // runs until stream reaches time `usize::MAX`
/// assert_eq!(values, output);
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

impl <V> IntoSource<NoKey, V, usize> for SingleIteratorSource<V>
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
    
            move |input: &mut Receiver<NoKey, NoData, NoTime>, output: &mut Sender<NoKey, V, usize>, _ctx| {
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
                        Message::ShutdownMarker(x) => output.send(Message::ShutdownMarker(x)),
                        Message::Interrogate(x) => output.send(Message::Interrogate(x)),
                        Message::Collect(x) => output.send(Message::Collect(x)),
                        Message::Acquire(x) => output.send(Message::Acquire(x)),
                        Message::DropKey(x) => output.send(Message::DropKey(x)),
                    }
                }
            }
        }
        
        )
    }
}

#[cfg(test)]
mod tests {

    use itertools::Itertools;

    use crate::{
        operators::{IntoSource, Sink, Source}, runtime::{threaded::MultiThreadRuntime, RuntimeBuilder, Worker}, sources::SingleIteratorSource, testing::{get_test_stream, VecSink}
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

        let c = collector
            .into_iter()
            .map(|x| x.value)
            .collect_vec();
        assert_eq!(c, (0..100).collect_vec())
    }

    /// It should only emit records on worker 0 to avoid duplicates
    #[test]
    fn emits_only_on_worker_0() {
        let sink = VecSink::new();
        
        let args = [sink.clone(), sink.clone()];

        let rt = MultiThreadRuntime::new_with_args(|flavor, this_sink| {
            let mut builder = RuntimeBuilder::new(flavor);
            builder.new_stream().source(SingleIteratorSource::new(0..10)).sink(this_sink).finish();
            builder
        }, args);
        
        rt.execute();
        // if both threads emitted values, we would expect this to contain 20 values, not 10
        assert_eq!(sink.len(), 10);
    }

    /// values should be timestamped with their iterator index
    #[test]
    fn emits_timestamped_messages() {
        todo!()
    }

    /// after the final value a single usize::MAX epoch should
    /// be emitted
    #[test]
    fn emits_max_epoch() {
        todo!()
    }

}
