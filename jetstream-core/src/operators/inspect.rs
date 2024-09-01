use crate::{stream::JetStreamBuilder, types::{Timestamp, Data, MaybeKey}};

use super::map::Map;

pub trait Inspect<K, V, T> {
    /// Observe values in a stream without modifying them.
    /// This is often done for debugging purposes or to record metrics.
    ///
    /// Inspect takes a closure of function which is called on every data
    /// message.
    ///
    /// To inspect the current event time see [`crate::operators::timely::InspectFrontier::inspect_frontier`].
    ///
    /// ```
    /// use tracing::debug;
    ///
    /// stream: JetStreamBuilder<NoKey, &str, NoTime, NoPersistence>
    /// stream.inspect(|x| debug!(x));
    ///
    /// ```
    fn inspect(self, inspector: impl FnMut(&V) + 'static) -> JetStreamBuilder<K, V, T>;
}

impl<K, V, T> Inspect<K, V, T> for JetStreamBuilder<K, V, T>
where
    K: MaybeKey,
    V: Data,
    T: Timestamp,
{
    fn inspect(self, mut inspector: impl FnMut(&V) + 'static) -> JetStreamBuilder<K, V, T> {
        self.map(move |x| {
            inspector(&x);
            x
        })
    }
}

#[cfg(test)]
mod tests {
    

    use itertools::Itertools;

    use crate::{
        operators::{
            inspect::Inspect,
            source::Source, Sink,
        }, sources::SingleIteratorSource, testing::{get_test_stream, VecSink}
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
            .source(SingleIteratorSource::new(input))
            .inspect(move |x| inspect_collector_moved.give(x.to_owned()))
            .sink(output_collector.clone())
            .finish();
        builder.build().unwrap().execute();

        assert_eq!(inspect_collector.drain_vec(..), expected);
        // check we still get unmodified output
        assert_eq!(output_collector.into_iter().map(|x| x.value).collect_vec(), expected);
    }
}
