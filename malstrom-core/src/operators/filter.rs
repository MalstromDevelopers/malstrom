use super::stateless_op::StatelessOp;
use crate::channels::operator_io::Output;
use crate::stream::StreamBuilder;
use crate::types::{Data, DataMessage, MaybeKey};
use crate::types::{Message, Timestamp};

/// Filter messages in a stream
pub trait Filter<K, V, T>: super::sealed::Sealed {
    /// Filters the datastream based on a given predicate.
    ///
    /// The given function receives an immutable reference to the value
    /// of every data message reaching this operator.
    /// If the function returns `true`, the message will be retained and
    /// passed downstream, if the function returns `false`, the message
    /// will be discarded.
    ///
    /// # Example
    ///
    /// Only retain numbers <= 42
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
    ///         .filter("filter", |x| *x <= 42)
    ///         .sink("sink", StatelessSink::new(sink_clone));
    ///     })
    ///     .execute()
    ///     .unwrap();
    /// let expected: Vec<i32> = (0..=42).collect();
    /// let out: Vec<i32> = sink.into_iter().map(|x| x.value).collect();
    /// assert_eq!(out, expected);
    /// ```
    fn filter(self, name: &str, filter: impl FnMut(&V) -> bool + 'static)
        -> StreamBuilder<K, V, T>;
}

impl<K, V, T> Filter<K, V, T> for StreamBuilder<K, V, T>
where
    K: MaybeKey,
    V: Data,
    T: Timestamp,
{
    fn filter(
        self,
        name: &str,

        mut filter: impl FnMut(&V) -> bool + 'static,
    ) -> StreamBuilder<K, V, T> {
        self.stateless_op(
            name,
            move |item: DataMessage<K, V, T>, out: &mut Output<K, V, T>| {
                if filter(&item.value) {
                    out.send(Message::Data(item))
                }
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        operators::*,
        sinks::StatelessSink,
        sources::{SingleIteratorSource, StatelessSource},
        testing::{get_test_rt, VecSink},
    };

    #[test]
    fn test_filter() {
        let collector = VecSink::new();
        let rt = get_test_rt(|provider| {
            provider
                .new_stream()
                .source(
                    "source",
                    StatelessSource::new(SingleIteratorSource::new(0..100)),
                )
                .filter("less-than-42", |x| *x < 42)
                .sink("sink", StatelessSink::new(collector.clone()));
        });
        rt.execute().unwrap();

        let collected: Vec<usize> = collector.into_iter().map(|x| x.value).collect();
        let expected: Vec<usize> = (0..42).collect();
        assert_eq!(expected, collected)
    }
}
