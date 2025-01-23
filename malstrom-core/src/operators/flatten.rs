use super::stateless_op::StatelessOp;
use crate::channels::operator_io::Output;
use crate::stream::JetStreamBuilder;
use crate::types::{Data, DataMessage, MaybeKey, Message, Timestamp};

pub trait Flatten<K, VI, T, VO, I>: super::sealed::Sealed {
    /// Flatten a datastream. Given a stream of some iterables, this function consumes
    /// each iterable and emits each of its elements downstream.
    ///
    /// # Key and Time
    /// If the message containing the iterator has a key or timestamp,
    /// they are cloned and attached to every emitted message.
    ///
    /// # Example
    ///
    /// Only retain numbers <= 42
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
    ///     .source(SingleIteratorSource::new([vec![1, 2, 3], vec![4, 5], vec![6]]))
    ///     .flatten()
    ///     .sink(sink_clone)
    ///     .finish();
    ///
    /// worker.build().expect("can build").execute();
    /// let expected: Vec<i32> = vec![1, 2, 3, 4, 5, 6];
    /// let out: Vec<i32> = sink.into_iter().map(|x| x.value).collect();
    /// assert_eq!(out, expected);
    /// ```
    fn flatten(self, name: &str) -> JetStreamBuilder<K, VO, T>;
}

impl<K, VI, T, VO, I> Flatten<K, VI, T, VO, I> for JetStreamBuilder<K, VI, T>
where
    K: MaybeKey,
    I: Iterator<Item = VO>,
    VI: IntoIterator<Item = VO, IntoIter = I> + Data,
    VO: Data,
    T: Timestamp,
{
    fn flatten(self, name: &str) -> JetStreamBuilder<K, VO, T> {
        self.stateless_op(
            name,
            move |item: DataMessage<K, VI, T>, out: &mut Output<K, VO, T>| {
                let key = item.key;
                let timestamp = item.timestamp;
                for x in item.value {
                    out.send(Message::Data(DataMessage::new(
                        key.clone(),
                        x,
                        timestamp.clone(),
                    )))
                }
            },
        )
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
    fn test_flatten() {
        let collector = VecSink::new();
        let rt = get_test_rt(|provider| {
            provider
                .new_stream()
                .source(
                    "source",
                    StatelessSource::new(SingleIteratorSource::new([
                        vec![1, 2],
                        vec![3, 4],
                        vec![5],
                    ])),
                )
                .flatten("flatten")
                .sink("sink", StatelessSink::new(collector.clone()));
        });
        rt.execute().unwrap();
        let result = collector.into_iter().map(|x| x.value).collect_vec();
        let expected = vec![1, 2, 3, 4, 5];
        assert_eq!(result, expected);
    }

    /// check we preserve the timestamp on every message
    #[test]
    fn test_preserves_time() {
        let collector = VecSink::new();
        let rt = get_test_rt(|provider| {
            provider
                .new_stream()
                .source(
                    "source",
                    StatelessSource::new(SingleIteratorSource::new([
                        vec![1, 2],
                        vec![3, 4],
                        vec![5],
                    ])),
                )
                .flatten("flatten")
                .sink("sink", StatelessSink::new(collector.clone()));
        });

        rt.execute().unwrap();
        let expected = vec![(1, 0), (2, 0), (3, 1), (4, 1), (5, 2)];
        let result = collector
            .into_iter()
            .map(|x| (x.value, x.timestamp))
            .collect_vec();
        assert_eq!(result, expected);
    }

    // check we preserve the key
    #[test]
    fn test_preserves_key() {
        let collector = VecSink::new();
        let rt = get_test_rt(|provider| {
            provider
                .new_stream()
                .source(
                    "source",
                    StatelessSource::new(SingleIteratorSource::new([
                        vec![1, 2],
                        vec![3, 4, 5],
                        vec![6],
                    ])),
                )
                .key_local("key-local", |x| x.value.len())
                .flatten("flatten")
                .sink("sink", StatelessSink::new(collector.clone()));
        });
        rt.execute().unwrap();
        let expected = vec![(1, 2), (2, 2), (3, 3), (4, 3), (5, 3), (6, 1)];
        let result = collector
            .into_iter()
            .map(|d| (d.value, d.key))
            .collect_vec();
        assert_eq!(result, expected);
    }
}
