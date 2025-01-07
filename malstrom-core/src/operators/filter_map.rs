use super::stateless_op::StatelessOp;
use crate::channels::operator_io::Output;
use crate::stream::JetStreamBuilder;
use crate::types::{Data, DataMessage, MaybeKey, Message, Timestamp};

pub trait FilterMap<K, VI, T>: super::sealed::Sealed {
    /// Applies a function to every element of the stream.
    /// All elements for which the function returns `Some(x)` are emitted downstream
    /// as `x`, all elements for which the function returns `None` are removed from
    /// the stream
    ///
    /// # Example
    ///
    /// Only retain numeric strings
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
    ///     .source(SingleIteratorSource::new(["0", "one", "2", "3", "four"]))
    ///     .filter_map(|x| x.parse::<i32>().ok())
    ///     .sink(sink_clone)
    ///     .finish();
    ///
    /// worker.build().expect("can build").execute();
    /// let expected: Vec<i32> = vec![0, 2, 3];
    /// let out: Vec<i32> = sink.into_iter().map(|x| x.value).collect();
    /// assert_eq!(out, expected);
    /// ```
    fn filter_map<VO: Data>(
        self,
        name: &str,

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
        name: &str,

        mut mapper: impl FnMut(VI) -> Option<VO> + 'static,
    ) -> JetStreamBuilder<K, VO, T> {
        self.stateless_op(
            name,
            move |item: DataMessage<K, VI, T>, out: &mut Output<K, VO, T>| {
                if let Some(x) = mapper(item.value) {
                    out.send(Message::Data(DataMessage::new(item.key, x, item.timestamp)))
                }
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        operators::{sink::Sink, source::Source},
        sources::{SingleIteratorSource, StatelessSource},
        testing::{get_test_stream, VecSink},
    };

    use super::*;
    #[test]
    fn test_filter_map() {
        let (builder, stream) = get_test_stream();
        let collector = VecSink::new();
        let stream = stream
            .source(
                "source",
                StatelessSource::new(SingleIteratorSource::new(0..100)),
            )
            .filter_map("less-than-42", |x| if x < 42 { Some(x * 2) } else { None })
            .sink("sink", collector.clone());
        stream.finish();
        let mut worker = builder.build().unwrap();

        worker.execute();

        let collected: Vec<usize> = collector.into_iter().map(|x| x.value).collect();
        let expected: Vec<usize> = (0..42).map(|x| x * 2).collect();
        assert_eq!(expected, collected)
    }
}
