use super::stateless_op::StatelessOp;
use crate::stream::JetStreamBuilder;

use crate::types::{Data, DataMessage, MaybeKey, Message, Timestamp};

pub trait Map<K, V, T, VO> {
    /// Map transforms every value in a datastream into a different value
    /// by applying a given function or closure.
    ///
    /// # Example
    /// ```
    /// stream: JetStreamBuilder<NoKey, &str, NoTime, NoPersistence>
    /// let lengths = JetStreamBuilder<NoKey, usize, NoTime, NoPersistence> = stream.map(|x| x.len())
    /// ```
    fn map(self, mapper: impl (FnMut(V) -> VO) + 'static) -> JetStreamBuilder<K, VO, T>;
}

impl<K, V, T, VO> Map<K, V, T, VO> for JetStreamBuilder<K, V, T>
where
    K: MaybeKey,
    V: Data,
    VO: Data,
    T: Timestamp,
{
    fn map(self, mut mapper: impl (FnMut(V) -> VO) + 'static) -> JetStreamBuilder<K, VO, T> {
        self.stateless_op(move |item, out| {
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
        operators::{
            map::Map,
            source::Source, Sink,
        }, sources::SingleIteratorSource, testing::{get_test_stream, VecSink}
    };

    #[test]
    fn test_map() {
        let (builder, stream) = get_test_stream();
        let input = ["hello", "world", "foo", "bar"];
        let expected = input.iter().map(|x| x.len()).collect_vec();
        let collector = VecSink::new();

        stream
            .source(SingleIteratorSource::new(input))
            .map(|x| x.len())
            .sink(collector.clone())
            .finish();
        builder.build().unwrap().execute();

        assert_eq!(collector.into_iter().map(|x| x.value).collect_vec(), expected);
    }
}
