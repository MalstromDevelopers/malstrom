use super::stateless_op::StatelessOp;
use crate::channels::operator_io::Output;
use crate::stream::{Malstrom, Operator, SafeLogic, StreamBuilder};
use crate::types::{Data, DataMessage, Kvt, MaybeKey, Message, Sealed, Timestamp};

/// Filter messages in a stream
pub trait Filter<In: Kvt>: Sealed {
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
        filter: impl FnMut(&In::Value) -> bool + 'static,
    ) -> StreamBuilder<In>;
}

impl<In> Filter<In> for StreamBuilder<In>
where
    In: Kvt,
{
    fn filter(
        self,
        name: impl Into<String>,
        filter: impl FnMut(&In::Value) -> bool + 'static,
    ) -> StreamBuilder<In> {
        let op = SafeLogic::<In, In>::into_logic(FilterOp(filter));
        self.then(Operator::direct(name.into(), op))
    }
}

struct FilterOp<F>(F);

impl<In, F> SafeLogic<In, In> for FilterOp<F>
where
    In: Kvt,
    F: FnMut(&In::Value) -> bool + 'static,
{
    fn on_data(
        &mut self,
        data_message: DataMessage<In>,
        output: &mut Output<In>,
        ctx: &mut crate::stream::OperatorContext,
    ) {
        if (self.0)(&data_message.value) {
            output.send(Message::Data(data_message))
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
                .filter("less-than-42", |x| *x < 42)
                .sink("sink", StatelessSink::new(collector.clone()));
        });
        rt.execute().unwrap();

        let collected: Vec<usize> = collector.into_iter().map(|x| x.value).collect();
        let expected: Vec<usize> = (0..42).collect();
        assert_eq!(expected, collected)
    }
}
