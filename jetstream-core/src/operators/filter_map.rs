use super::stateless_op::StatelessOp;
use crate::stream::JetStreamBuilder;
use crate::types::{Data, DataMessage, MaybeKey, Message, Timestamp};

pub trait FilterMap<K, VI, T> {
    /// Applies a function to every element of the stream.
    /// All elements for which the function returns `Some(x)` are emitted downstream
    /// as `x`, all elements for which the function returns `None` are removed from
    /// the stream
    ///
    /// # Example
    ///
    /// Only retain numeric strings
    /// ```
    /// stream: JetStreamBuilder<NoKey, String, NoTime, NoPersistence>
    /// stream.filter_map(|x| x.parse::<i64>().ok())
    /// ````
    fn filter_map<VO: Data>(
        self,
        mapper: impl FnMut(VI) -> Option<VO> + 'static,
    ) -> JetStreamBuilder<K, VO, T>;
}

impl<K, VI, T> FilterMap<K, VI, T> for JetStreamBuilder<K, VI, T>
where
    K: MaybeKey,
    VI: Data,
    T: Timestamp,
{
    fn filter_map<VO: Data>(
        self,
        mut mapper: impl FnMut(VI) -> Option<VO> + 'static,
    ) -> JetStreamBuilder<K, VO, T> {
        self.stateless_op(move |item, out| {
            if let Some(x) = mapper(item.value) {
                out.send(Message::Data(DataMessage::new(item.key, x, item.timestamp)))
            }
        })
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
    fn test_filter_map() {
        let (builder, stream) = get_test_stream();
        let collector = VecSink::new();

        let stream = stream
            .source(SingleIteratorSource::new(0..100))
            .filter_map(|x| if x < 42 { Some(x * 2) } else { None })
            .sink(collector.clone());
        stream.finish();
        let mut worker = builder.build().unwrap();

        worker.execute();

        let collected: Vec<usize> = collector.into_iter().map(|x| x.value).collect();
        let expected: Vec<usize> = (0..42).map(|x| x * 2).collect();
        assert_eq!(expected, collected)
    }
}
