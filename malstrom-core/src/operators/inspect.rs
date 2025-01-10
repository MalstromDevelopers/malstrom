use crate::{
    stream::{JetStreamBuilder, OperatorBuilder, OperatorContext},
    types::{Data, DataMessage, MaybeKey, Message, Timestamp},
};

pub trait Inspect<K, V, T>: super::sealed::Sealed {
    /// Observe values in a stream without modifying them.
    /// This is often done for debugging purposes or to record metrics.
    ///
    /// Inspect takes a closure of function which is called on every data
    /// message.
    ///
    /// To inspect the current event time see [`crate::operators::timely::InspectFrontier::inspect_frontier`].
    ///
    /// ```rust
    /// use malstrom::operators::*;
    /// use malstrom::operators::Source;
    /// use malstrom::runtime::{WorkerBuilder, threaded::SingleThreadRuntimeFlavor};
    /// use malstrom::testing::VecSink;
    /// use malstrom::sources::SingleIteratorSource;
    ///
    /// let sink = VecSink::new();
    /// let sink_clone = sink.clone();
    ///
    /// let mut worker = WorkerBuilder::new(SingleThreadRuntimeFlavor::default());
    ///
    /// worker
    ///     .new_stream()
    ///     .source(SingleIteratorSource::new(0..100))
    ///     .inspect(move |msg, _ctx| sink_clone.give(msg.clone()))
    ///     .finish();
    ///
    /// worker.build().expect("can build").execute();
    /// let expected: Vec<i32> = (0..100).collect();
    /// let out: Vec<i32> = sink.into_iter().map(|x| x.value).collect();
    /// assert_eq!(out, expected);
    /// ```
    fn inspect(
        self,
        name: &str,

        inspector: impl FnMut(&DataMessage<K, V, T>, &OperatorContext) -> () + 'static,
    ) -> JetStreamBuilder<K, V, T>;
}

impl<K, V, T> Inspect<K, V, T> for JetStreamBuilder<K, V, T>
where
    K: MaybeKey,
    V: Data,
    T: Timestamp,
{
    fn inspect(
        self,
        name: &str,

        mut inspector: impl FnMut(&DataMessage<K, V, T>, &OperatorContext) -> () + 'static,
    ) -> JetStreamBuilder<K, V, T> {
        let operator =
            OperatorBuilder::direct(name, move |input, output, ctx| match input.recv() {
                Some(Message::Data(d)) => {
                    inspector(&d, ctx);
                    output.send(Message::Data(d));
                }
                Some(x) => output.send(x),
                None => (),
            });
        self.then(operator)
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use crate::{
        operators::{inspect::Inspect, source::Source, Sink},
        sinks::StatelessSink,
        sources::{SingleIteratorSource, StatelessSource},
        testing::{get_test_stream, VecSink},
    };

    #[test]
    fn test_inspect() {
        let (builder, stream) = get_test_stream();

        let inspect_collector = VecSink::new();
        let inspect_collector_moved = inspect_collector.clone();

        let output_collector = VecSink::new();

        let input = vec!["hello", "world", "foo", "bar"];
        let expected = input.clone();
        stream
            .source(
                "source",
                StatelessSource::new(SingleIteratorSource::new(input)),
            )
            .inspect("inspect", move |x, _ctx| {
                inspect_collector_moved.give(x.value.to_owned())
            })
            .sink("sink", StatelessSink::new(output_collector.clone()));
        builder.build().unwrap().execute();

        assert_eq!(inspect_collector.drain_vec(..), expected);
        // check we still get unmodified output
        assert_eq!(
            output_collector.into_iter().map(|x| x.value).collect_vec(),
            expected
        );
    }
}
