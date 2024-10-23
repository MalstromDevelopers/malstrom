use super::stateless_op::StatelessOp;
use crate::channels::selective_broadcast::Sender;
use crate::stream::JetStreamBuilder;
use crate::types::{Data, DataMessage, MaybeKey};
use crate::types::{Message, Timestamp};

pub trait Filter<K, V, T> {
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
    /// use jetstream::operators::*;
    /// use jetstream::operators::Source;
    /// use jetstream::runtime::{WorkerBuilder, threaded::SingleThreadRuntimeFlavor};
    /// use jetstream::testing::VecSink;
    /// use jetstream::sources::SingleIteratorSource;
    ///
    /// let sink = VecSink::new();
    /// let sink_clone = sink.clone();
    ///
    /// let mut worker = WorkerBuilder::new(SingleThreadRuntimeFlavor::default());
    ///
    /// worker
    ///     .new_stream()
    ///     .source(SingleIteratorSource::new(0..100))
    ///     .filter(|x| *x <= 42)
    ///     .sink(sink_clone)
    ///     .finish();
    ///
    /// worker.build().expect("can build").execute();
    /// let expected: Vec<i32> = (0..=42).collect();
    /// let out: Vec<i32> = sink.into_iter().map(|x| x.value).collect();
    /// assert_eq!(out, expected);
    /// ```
    fn filter(self, filter: impl FnMut(&V) -> bool + 'static) -> JetStreamBuilder<K, V, T>;
}

impl<K, V, T> Filter<K, V, T> for JetStreamBuilder<K, V, T>
where
    K: MaybeKey,
    V: Data,
    T: Timestamp,
{
    fn filter(self, mut filter: impl FnMut(&V) -> bool + 'static) -> JetStreamBuilder<K, V, T> {
        self.stateless_op(
            move |item: DataMessage<K, V, T>, out: &mut Sender<K, V, T>| {
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
        operators::{sink::Sink, source::Source},
        sources::SingleIteratorSource,
        testing::{get_test_stream, VecSink},
    };

    use super::*;
    #[test]
    fn test_filter() {
        let (builder, stream) = get_test_stream();

        let collector = VecSink::new();

        let stream = stream
            .source(SingleIteratorSource::new(0..100))
            .filter(|x| *x < 42)
            .sink(collector.clone());
        stream.finish();
        let mut worker = builder.build().unwrap();

        worker.execute();

        let collected: Vec<usize> = collector.into_iter().map(|x| x.value).collect();
        let expected: Vec<usize> = (0..42).collect();
        assert_eq!(expected, collected)
    }
}
