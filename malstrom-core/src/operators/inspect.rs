use crate::{
    stream::{OperatorBuilder, OperatorContext, StreamBuilder},
    types::{Data, DataMessage, MaybeKey, Message, Timestamp},
};

/// Inspect messages in a stream without modifying them
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

        inspector: impl FnMut(&DataMessage<K, V, T>, &OperatorContext) + 'static,
    ) -> StreamBuilder<K, V, T>;
}

impl<K, V, T> Inspect<K, V, T> for StreamBuilder<K, V, T>
where
    K: MaybeKey,
    V: Data,
    T: Timestamp,
{
    fn inspect(
        self,
        name: &str,

        mut inspector: impl FnMut(&DataMessage<K, V, T>, &OperatorContext) + 'static,
    ) -> StreamBuilder<K, V, T> {
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
        operators::*,
        sinks::StatelessSink,
        sources::{SingleIteratorSource, StatelessSource},
        testing::{get_test_rt, VecSink},
    };

    #[test]
    fn test_inspect() {
        let inspect_collector = VecSink::new();
        let output_collector = VecSink::new();

        let input = vec!["hello", "world", "foo", "bar"];
        let expected = input.clone();

        let rt = get_test_rt(|provider| {
            let inspect_collector = inspect_collector.clone();
            provider
                .new_stream()
                .source(
                    "source",
                    StatelessSource::new(SingleIteratorSource::new(input.clone())),
                )
                .inspect("inspect", move |x, _| {
                    inspect_collector.give(x.value.to_owned())
                })
                .sink("sink", StatelessSink::new(output_collector.clone()));
        });
        rt.execute().unwrap();
        assert_eq!(inspect_collector.drain_vec(..), expected);
        // check we still get unmodified output
        assert_eq!(
            output_collector.into_iter().map(|x| x.value).collect_vec(),
            expected
        );
    }
}
