use super::stateless_op::StatelessOp;
use crate::stream::JetStreamBuilder;
use crate::types::{Data, DataMessage, MaybeKey, Message, Timestamp};

pub trait Flatten<K, VI, T, VO, I> {
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
    /// ```
    /// stream: JetStreamBuilder<NoKey, Vec<i64>, NoTime, NoPersistence>
    /// let flat: JetStreamBuilder<NoKey, i64, NoTime, NoPersistence> = stream.flatten();
    ///
    /// ````
    fn flatten(self) -> JetStreamBuilder<K, VO, T>;
}

impl<K, VI, T, VO, I> Flatten<K, VI, T, VO, I> for JetStreamBuilder<K, VI, T>
where
    K: MaybeKey,
    I: Iterator<Item = VO>,
    VI: IntoIterator<Item = VO, IntoIter = I> + Data,
    VO: Data,
    T: Timestamp,
{
    fn flatten(self) -> JetStreamBuilder<K, VO, T> {
        self.stateless_op(move |item, out| {
            let key = item.key;
            let timestamp = item.timestamp;
            for x in item.value {
                out.send(Message::Data(DataMessage::new(
                    key.clone(),
                    x,
                    timestamp.clone(),
                )))
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use crate::{
        operators::{
            source::Source,
            KeyLocal, Sink,
        },
        sources::SingleIteratorSource,
        testing::{get_test_stream, VecSink},
    };

    use super::Flatten;
    #[test]
    fn test_flatten() {
        let (builder, stream) = get_test_stream();

        let collector = VecSink::new();
        stream
            .source(SingleIteratorSource::new([vec![1, 2], vec![3, 4], vec![5]]))
            .flatten()
            .sink(collector.clone())
            .finish();
        builder.build().unwrap().execute();

        let result = collector.into_iter().map(|x| x.value).collect_vec();
        let expected = vec![1, 2, 3, 4, 5];
        assert_eq!(result, expected);
    }

    /// check we preserve the timestamp on every message
    #[test]
    fn test_preserves_time() {
        let (builder, stream) = get_test_stream();
        let collector = VecSink::new();
        stream
            .source(SingleIteratorSource::new([vec![1, 2], vec![3, 4], vec![5]]))
            .flatten()
            .sink(collector.clone())
            .finish();

        builder.build().unwrap().execute();
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
        let (builder, stream) = get_test_stream();
        let collector = VecSink::new();
        stream
            .source(SingleIteratorSource::new([
                vec![1, 2],
                vec![3, 4, 5],
                vec![6],
            ]))
            .key_local(|x| x.value.len())
            .flatten()
            .sink(collector.clone())
            .finish();
        builder.build().unwrap().execute();
        let expected = vec![(1, 2), (2, 2), (3, 3), (4, 3), (5, 3), (6, 1)];
        let result = collector
            .into_iter()
            .map(|d| (d.value, d.key))
            .collect_vec();
        assert_eq!(result, expected);
    }
}
