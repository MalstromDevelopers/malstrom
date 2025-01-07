use crate::{
    operators::IntoSource, stream::OperatorBuilder, types::{Data, DataMessage, Message, NoData, NoKey, NoTime}
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
/// ```
/// # use malstrom::sources::SingleIteratorSource;
/// use malstrom::{JetStreamBuilder, RuntimeBuilder};
/// use malstrom::test::TestingValuesSink;
/// use malstrom::runtimes::SingleThreadRuntime;
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
        let mut inner = self.iterator.enumerate();
        let mut final_emitted = false;
        OperatorBuilder::direct(move |input, output, _ctx| {
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
        })
    }
}

#[cfg(test)]
mod tests {

    use itertools::Itertools;

    use crate::{
        operators::{Sink, Source}, sources::SingleIteratorSource, testing::{get_test_stream, VecSink}
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
        todo!()
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
