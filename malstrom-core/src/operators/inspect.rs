use std::marker::PhantomData;

use crate::{
    channels::operator_io::{Input, Output},
    stream::{Malstrom as _, Operator, OperatorContext, SafeLogic, StreamBuilder},
    types::{Data, DataMessage, Kvt, MaybeKey, Message, Sealed, Timestamp},
};

/// Inspect messages in a stream without modifying them
pub trait Inspect<Msg: Kvt, Inspector>: Sealed {
    /// Observe values in a stream without modifying them.
    /// This is often done for debugging purposes or to record metrics.
    ///
    /// Inspect takes a closure of function which is called on every data
    /// message.
    ///
    /// To inspect the current event time see [`crate::operators::timely::InspectFrontier::inspect_frontier`].
    ///
    /// ```rust
    /// use malstrom::operators::*;
    /// use malstrom::runtime::SingleThreadRuntime;
    /// use malstrom::snapshot::NoPersistence;
    /// use malstrom::sources::{SingleIteratorSource, StatelessSource};
    /// use malstrom::worker::StreamProvider;
    /// use malstrom::sinks::{VecSink, StatelessSink};
    ///
    /// let sink = VecSink::new();
    /// let sink_insepct = sink.clone();
    ///
    /// let sink_output = VecSink::new();
    ///
    /// SingleThreadRuntime::builder()
    ///     .persistence(NoPersistence)
    ///     .build(move |provider: &mut dyn StreamProvider| {
    ///         provider.new_stream()
    ///         .source("numbers", StatelessSource::new(SingleIteratorSource::new(0..100)))
    ///         .
    /// inspect("inspect", async move |msg, _ctx| sink_insepct.give(msg.clone()))
    ///         .sink("sink", StatelessSink::new(sink_output));
    ///     })
    ///     .execute()
    ///     .unwrap();
    ///
    /// let expected: Vec<i32> = (0..100).collect();
    /// let out: Vec<i32> = sink.into_iter().map(|x| x.value).collect();
    /// assert_eq!(out, expected);
    /// ```
    fn inspect(self, name: impl Into<String>, inspector: Inspector) -> StreamBuilder<Msg>;
}

impl<Msg, Inspector, Fut> Inspect<Msg, Inspector> for StreamBuilder<Msg>
where
    Msg: Kvt,
    Inspector: (FnMut(&DataMessage<Msg>, &OperatorContext) -> Fut) + 'static,
    Fut: Future<Output = ()>,
{
    fn inspect(self, name: impl Into<String>, mut inspector: Inspector) -> StreamBuilder<Msg> {
        let operator = Operator::direct(
            name.into(),
            InspectOp {
                func: inspector,
                _msg: PhantomData::<Msg>,
            }
            .into_logic(),
        );
        self.then(operator)
    }
}

struct InspectOp<Msg: Kvt, Inspector> {
    func: Inspector,
    _msg: PhantomData<Msg>,
}

impl<Msg, Inspector, Fut> SafeLogic<Msg, Msg> for InspectOp<Msg, Inspector>
where
    Msg: Kvt,
    Inspector: (FnMut(&DataMessage<Msg>, &OperatorContext) -> Fut) + 'static,
    Fut: Future<Output = ()>,
{
    async fn on_data(
        &mut self,
        data_message: DataMessage<Msg>,
        output: &mut Output<Msg>,
        ctx: &mut OperatorContext<'_>,
    ) {
        (self.func)(&data_message, ctx).await;
        output.send(Message::Data(data_message));
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use crate::{
        operators::*,
        sinks::StatelessSink,
        sources::{SingleIteratorSource, StatelessSource},
        testing::{VecSink, get_test_rt},
    };

    #[test]
    fn test_inspect() {
        let inspect_collector = VecSink::new();
        let output_collector = VecSink::new();

        let input = vec!["hello", "world", "foo", "bar"];
        let expected = input.clone();

        let rt = get_test_rt(|provider| {
            let inspect_collector = inspect_collector.clone();
            provider
                .new_stream()
                .source(
                    "source",
                    StatelessSource::new(SingleIteratorSource::new(input.clone())),
                )
                .inspect("inspect", move |x, _| {
                    inspect_collector.give(x.value.to_owned())
                })
                .sink("sink", StatelessSink::new(output_collector.clone()));
        });
        rt.execute().unwrap();
        assert_eq!(inspect_collector.drain_vec(..), expected);
        // check we still get unmodified output
        assert_eq!(
            output_collector.into_iter().map(|x| x.value).collect_vec(),
            expected
        );
    }
}
