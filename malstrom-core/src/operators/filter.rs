use super::stateless_op::StatelessOp;
use crate::channels::operator_io::Output;
use crate::msg;
use crate::operators::StatelessLogic;
use crate::stream::StreamBuilder;
use crate::types::{Data, DataMessage, Kvt, MaybeKey, Message, Sealed, Timestamp};

/// Filter messages in a stream
pub trait Filter<In: Kvt, FilterFunc>: Sealed {
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
    fn filter(
        self,
        name: impl Into<String>,
        filter: FilterFunc,
    ) -> StreamBuilder<(In::Key, In::Value, In::Timestamp)>;
}

impl<In, FilterFunc> Filter<In, FilterFunc> for StreamBuilder<In>
where
    In: Kvt,
    FilterFunc: AsyncFnMut(&In::Value) -> bool + 'static,
{
    fn filter(
        self,
        name: impl Into<String>,
        filter: FilterFunc,
    ) -> StreamBuilder<(In::Key, In::Value, In::Timestamp)> {
        self.stateless_op(name.into(), FilterOp(filter))
    }
}

struct FilterOp<FilterFunc>(FilterFunc);

impl<In, FilterFunc> StatelessLogic<In, In::Value> for FilterOp<FilterFunc>
where
    In: Kvt,
    FilterFunc: AsyncFnMut(&In::Value) -> bool + 'static,
{
    async fn on_data(
        &mut self,
        msg: DataMessage<In>,
        output: &mut Output<(In::Key, In::Value, In::Timestamp)>,
    ) {
        if (self.0)(&msg.value).await {
            // this is needed to get the output type right
            let out_msg = DataMessage::new(msg.key, msg.value, msg.timestamp);
            output.send(Message::Data(out_msg)).await
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        operators::*,
        sinks::StatelessSink,
        sources::{SingleIteratorSource, StatelessSource},
        testing::{VecSink, get_test_rt},
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
                .filter("less-than-42", async |x| *x < 42)
                .sink("sink", StatelessSink::new(collector.clone()));
        });
        rt.execute().unwrap();

        let collected: Vec<usize> = collector.into_iter().map(|x| x.value).collect();
        let expected: Vec<usize> = (0..42).collect();
        assert_eq!(expected, collected)
    }
}
