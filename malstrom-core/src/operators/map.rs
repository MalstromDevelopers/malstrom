use super::stateless_op::StatelessOp;
use crate::channels::operator_io::Output;
use crate::stream::StreamBuilder;
use crate::types::{Data, DataMessage, Kvt, MaybeKey, Message, Sealed, Timestamp};

/// Apply a function to every message in a stream
pub trait Map<In: Kvt, T: Data>: Sealed {
    /// Map transforms every value in a datastream into a different value
    /// by applying a given function or closure.
    ///
    /// # Example
    /// ```rust
    /// use malstrom::operators::*;
    /// use malstrom::runtime::SingleThreadRuntime;
    /// use malstrom::snapshot::NoPersistence;
    /// use malstrom::sources::{SingleIteratorSource, StatelessSource};
    /// use malstrom::worker::StreamProvider;
    /// use malstrom::sinks::{VecSink, StatelessSink};
    ///
    /// let sink = VecSink::new();
    /// let sink_clone = sink.clone();
    ///
    /// SingleThreadRuntime::builder()
    ///     .persistence(NoPersistence)
    ///     .build(move |provider: &mut dyn StreamProvider| {
    ///         provider.new_stream()
    ///         .source("numbers", StatelessSource::new(SingleIteratorSource::new(0..100)))
    ///         .map("map", |x| x * 2)
    ///         .sink("sink", StatelessSink::new(sink_clone));
    ///     })
    ///     .execute()
    ///     .unwrap();
    ///
    /// let expected: Vec<i32> = (0..100).map(|x| x * 2).collect();
    /// let out: Vec<i32> = sink.into_iter().map(|x| x.value).collect();
    /// assert_eq!(out, expected);
    /// ```
    fn map(
        self,
        name: &str,
        mapper: impl (FnMut(In::Value) -> T) + 'static,
    ) -> StreamBuilder<(In::Key, T, In::Timestamp)>;
}

impl<In, T> Map<In, T> for StreamBuilder<In>
where
    In: Kvt,
    T: Data,
{
    fn map(
        self,
        name: &str,
        mut mapper: impl (FnMut(In::Value) -> T) + 'static,
    ) -> StreamBuilder<(In::Key, T, In::Timestamp)> {
        self.stateless_op(name, move |item: DataMessage<In>, out: &mut Output<_>| {
            out.send(Message::Data(DataMessage::new(
                item.key,
                mapper(item.value),
                item.timestamp,
            )));
        })
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use crate::{
        operators::{Sink, map::Map, source::Source},
        sinks::StatelessSink,
        sources::{SingleIteratorSource, StatelessSource},
        testing::{VecSink, get_test_rt},
    };

    #[test]
    fn test_map() {
        let input = ["hello", "world", "foo", "bar"];
        let expected = input.iter().map(|x| x.len()).collect_vec();
        let collector = VecSink::new();

        let rt = get_test_rt(|provider| {
            provider
                .new_stream()
                .source(
                    "source",
                    StatelessSource::new(SingleIteratorSource::new(input)),
                )
                .map("get-len", |x| x.len())
                .sink("sink", StatelessSink::new(collector.clone()));
        });
        rt.execute().unwrap();
        assert_eq!(
            collector.into_iter().map(|x| x.value).collect_vec(),
            expected
        );
    }
}
