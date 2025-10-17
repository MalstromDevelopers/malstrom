use super::stateless_op::StatelessOp;
use crate::channels::operator_io::Output;
use crate::operators::StatelessLogic;
use crate::stream::StreamBuilder;
use crate::types::{Data, DataMessage, Kvt, MaybeKey, Message, Sealed, Timestamp};

/// Filter messages in a stream while at the same time applying a function to all values.
pub trait FilterMap<In: Kvt, T: Data, Mapper>: Sealed {
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
    ///         .source("numbers", StatelessSource::new(
    ///             SingleIteratorSource::new(["0", "one", "2", "3", "four"])
    ///         ))
    ///         .filter_map("filter_map", |x| x.parse::<i32>().ok())
    ///         .sink("sink", StatelessSink::new(sink_clone));
    ///     })
    ///     .execute()
    ///     .unwrap();
    ///
    /// let expected: Vec<i32> = vec![0, 2, 3];
    /// let out: Vec<i32> = sink.into_iter().map(|x| x.value).collect();
    /// assert_eq!(out, expected);
    /// ```
    fn filter_map(self, name: &str, mapper: Mapper) -> StreamBuilder<(In::Key, T, In::Timestamp)>;
}

impl<In, T, Mapper, Fut> FilterMap<In, T, Mapper> for StreamBuilder<In>
where
    In: Kvt,
    T: Data,
    Mapper: FnMut(In::Value) -> Fut + 'static,
    Fut: Future<Output = Option<T>>,
{
    fn filter_map(self, name: &str, mapper: Mapper) -> StreamBuilder<(In::Key, T, In::Timestamp)> {
        self.stateless_op(name, FilterMapOp { mapper })
    }
}

struct FilterMapOp<Mapper> {
    mapper: Mapper,
}
impl<In, Mapper, Fut, T> StatelessLogic<In, T> for FilterMapOp<Mapper>
where
    In: Kvt,
    T: Data,
    Mapper: FnMut(In::Value) -> Fut + 'static,
    Fut: Future<Output = Option<T>>,
{
    async fn on_data(
        &mut self,
        msg: DataMessage<In>,
        output: &mut Output<(<In as Kvt>::Key, T, <In as Kvt>::Timestamp)>,
    ) {
        if let Some(x) = (self.mapper)(msg.value).await {
            output.send(Message::Data(DataMessage::new(msg.key, x, msg.timestamp)))
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        operators::{sink::Sink, source::Source},
        sinks::StatelessSink,
        sources::{SingleIteratorSource, StatelessSource},
        testing::{VecSink, get_test_rt},
    };

    use super::*;
    #[test]
    fn test_filter_map() {
        let collector = VecSink::new();
        let rt = get_test_rt(|provider| {
            provider
                .new_stream()
                .source(
                    "source",
                    StatelessSource::new(SingleIteratorSource::new(0..100)),
                )
                .filter_map("less-than-42", |x| if x < 42 { Some(x * 2) } else { None })
                .sink("sink", StatelessSink::new(collector.clone()));
        });
        rt.execute().unwrap();

        let collected: Vec<usize> = collector.into_iter().map(|x| x.value).collect();
        let expected: Vec<usize> = (0..42).map(|x| x * 2).collect();
        assert_eq!(expected, collected)
    }
}
