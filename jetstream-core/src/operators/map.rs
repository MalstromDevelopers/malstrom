use super::stateless_op::StatelessOp;
use crate::channels::operator_io::Output;
use crate::stream::JetStreamBuilder;

use crate::types::{Data, DataMessage, MaybeKey, Message, Timestamp};

pub trait Map<K, V, T, VO>: super::sealed::Sealed {
    /// Map transforms every value in a datastream into a different value
    /// by applying a given function or closure.
    ///
    /// # Example
    /// ```rust
    /// use jetstream::operators::*;
    /// use jetstream::operators::Source;
    /// use jetstream::runtime::{WorkerBuilder, threaded::SingleThreadRuntimeFlavor};
    /// use jetstream::testing::VecSink;
    /// use jetstream::sources::SingleIteratorSource;
    ///
    /// let sink = VecSink::new();
    ///
    /// let mut worker = WorkerBuilder::new(SingleThreadRuntimeFlavor::default());
    ///
    /// worker
    ///     .new_stream()
    ///     .source(SingleIteratorSource::new(0..100))
    ///     .map(|x| x * 2)
    ///     .sink(sink.clone())
    ///     .finish();
    ///
    /// worker.build().expect("can build").execute();
    /// let expected: Vec<i32> = (0..100).map(|x| x * 2).collect();
    /// let out: Vec<i32> = sink.into_iter().map(|x| x.value).collect();
    /// assert_eq!(out, expected);
    /// ```
    fn map(self,name: &str, mapper: impl (FnMut(V) -> VO) + 'static) -> JetStreamBuilder<K, VO, T>;
}

impl<K, V, T, VO> Map<K, V, T, VO> for JetStreamBuilder<K, V, T>
where
    K: MaybeKey,
    V: Data,
    VO: Data,
    T: Timestamp,
{
    fn map(self, 
        name: &str,
        mut mapper: impl (FnMut(V) -> VO) + 'static) -> JetStreamBuilder<K, VO, T> {
        self.stateless_op(
            name,
            move |item: DataMessage<K, V, T>, out: &mut Output<K, VO, T>| {
                out.send(Message::Data(DataMessage::new(
                    item.key,
                    mapper(item.value),
                    item.timestamp,
                )));
            },
        )
    }
}
#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use crate::{
        operators::{map::Map, source::Source, Sink},
        sources::SingleIteratorSource,
        testing::{get_test_stream, VecSink},
    };

    #[test]
    fn test_map() {
        let (builder, stream) = get_test_stream();
        let input = ["hello", "world", "foo", "bar"];
        let expected = input.iter().map(|x| x.len()).collect_vec();
        let collector = VecSink::new();

        stream
            .source("source", SingleIteratorSource::new(input))
            .map("get-len", |x| x.len())
            .sink("sink",collector.clone())
            .finish();
        builder.build().unwrap().execute();

        assert_eq!(
            collector.into_iter().map(|x| x.value).collect_vec(),
            expected
        );
    }
}
