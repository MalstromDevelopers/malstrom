use std::marker::PhantomData;

use crate::{
    stream::{Logic, Malstrom as _, Operator, SafeLogic, StreamBuilder},
    types::{Data, DataMessage, Kvt, MaybeKey, Message, Sealed, Timestamp},
};

use super::NeedsEpochs;
/// Wrapper for messages which are either before the last epoch (on time)
/// or equal to or after the last epoch (late)
#[derive(Clone)]
pub(super) enum OnTimeLate<V> {
    OnTime(V),
    Late(V),
}

/// Assign timestamps to stream messages
pub trait AssignTimestamps<Msg: Kvt>: Sealed {
    /// Assigns a new timestamp to every message.
    /// NOTE: Any Epochs arriving at this operator are dropped with the exception
    /// of the `MAX` epoch. See [Timestamp::MAX]
    fn assign_timestamps<TO: Timestamp>(
        self,
        name: impl Into<String>,
        assigner: impl FnMut(&DataMessage<Msg>) -> TO + 'static,
    ) -> NeedsEpochs<(Msg::Key, Msg::Value, TO)>;
}

impl<Msg> AssignTimestamps<Msg> for StreamBuilder<Msg>
where
    Msg: Kvt,
    Msg::Value: Data,
    Msg::Timestamp: Timestamp,
{
    fn assign_timestamps<TO: Timestamp>(
        self,
        name: impl Into<String>,
        mut assigner: impl FnMut(&DataMessage<Msg>) -> TO + 'static,
    ) -> NeedsEpochs<(Msg::Key, Msg::Value, TO)> {
        let operator = Operator::direct(
            name.into(),
            AssignTimestampsOp {
                assigner,
                _timestamp_type: PhantomData::<TO>,
            },
        );
        NeedsEpochs(self.then(operator))
    }
}

struct AssignTimestampsOp<F, T> {
    assigner: F,
    _timestamp_type: PhantomData<T>,
}
impl<In, Out, F, T> Logic<In, Out> for AssignTimestampsOp<F, T>
where
    In: Kvt,
    In::Timestamp: Timestamp,
    Out: Kvt<Key = In::Key, Value = In::Value, Timestamp = T>,
    T: Timestamp,
    F: FnMut(&DataMessage<In>) -> T + 'static,
{
    async fn apply(
        &mut self,
        input: &mut crate::channels::operator_io::Input<In>,
        output: &mut crate::channels::operator_io::Output<Out>,
        ctx: &mut crate::stream::OperatorContext,
    ) {
        match input.recv().await {
            Message::Data(d) => {
                let timestamp = (self.assigner)(&d);
                let new = DataMessage::new(d.key, d.value, timestamp);
                output.send(Message::Data(new)).await
            }
            Message::Epoch(e) => {
                if e == In::Timestamp::MAX {
                    output.send(Message::Epoch(T::MAX)).await
                }
            }
            Message::Interrogate(x) => output.send(Message::Interrogate(x)).await,
            Message::Collect(c) => output.send(Message::Collect(c)).await,
            Message::Acquire(a) => output.send(Message::Acquire(a)).await,
            Message::AbsBarrier(b) => output.send(Message::AbsBarrier(b)).await,
            Message::Rescale(x) => output.send(Message::Rescale(x)).await,
            Message::ReconfigComplete(x) => output.send(Message::ReconfigComplete(x)).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        channels::operator_io::{Input, Output},
        operators::{GenerateEpochs, Sink, Source},
        sinks::StatelessSink,
        sources::{SingleIteratorSource, StatelessSource},
        stream::{DirectLogic, Operator, OperatorContext, SafeLogicWrapper},
        testing::{VecSink, get_test_rt},
        types::{MaybeData, MaybeTime, Message, NoKey},
    };
    use itertools::Itertools;

    use super::*;

    struct EpochCollector<Msg: Kvt>(VecSink<Msg::Timestamp>);

    impl<Msg> SafeLogic<Msg, Msg> for EpochCollector<Msg>
    where
        Msg: Kvt,
    {
        async fn on_data(
            &mut self,
            data_message: DataMessage<Msg>,
            output: &mut crate::channels::operator_io::Output<Msg>,
            ctx: &mut crate::stream::OperatorContext,
        ) {
            output.send(Message::Data(data_message));
        }

        async fn on_epoch(
            &mut self,
            epoch: &<Msg as Kvt>::Timestamp,
            output: &mut crate::channels::operator_io::Output<Msg>,
            ctx: &mut crate::stream::OperatorContext,
        ) {
            self.0.give(epoch.clone());
        }
    }

    fn epoch_collector<Msg: Kvt>(
        name: &str,
        collector: VecSink<Msg::Timestamp>,
    ) -> Operator<Msg, DirectLogic<SafeLogicWrapper<EpochCollector<Msg>>>, Msg>
    where
        Msg::Timestamp: Clone,
    {
        Operator::direct(name.into(), EpochCollector(collector).into_logic())
    }

    /// Check that the assigner assigns a timestamp to every record
    #[test]
    fn test_timestamp_gets_assigned() {
        let collector = VecSink::new();
        let rt = get_test_rt(|provider| {
            let (ontime, _late) = provider
                .new_stream()
                .source(
                    "source",
                    StatelessSource::new(SingleIteratorSource::new(0..10)),
                )
                .assign_timestamps("ts-double-value", |x| x.value * 2)
                .generate_epochs("no-epochs", |_x, _y| None);
            ontime.sink("sink", StatelessSink::new(collector.clone()));
        });

        rt.execute().unwrap();
        let timestamps = collector.into_iter().map(|x| x.timestamp).collect_vec();

        assert_eq!((0..10).map(|x| x * 2).collect_vec(), timestamps)
    }

    /// check epochs get issued according to the given function
    #[test]
    fn test_epoch_gets_issued() {
        let collector = VecSink::new();
        let late_collector = VecSink::new();

        let rt = get_test_rt(|provider| {
            let collector = collector.clone();
            let late_collector = late_collector.clone();

            let (stream, late) = provider
                .new_stream()
                .source(
                    "source",
                    StatelessSource::new(SingleIteratorSource::new(0..10)),
                )
                .assign_timestamps("ts-from-value", |x| x.value)
                .generate_epochs("add-epoch", |msg, epoch| {
                    Some(msg.timestamp + epoch.unwrap_or(0))
                });

            stream.then(epoch_collector("get-epoch", collector));
            late.then(epoch_collector("get-epoch-late", late_collector));
        });

        rt.execute().unwrap();

        let timestamps: Vec<i32> = collector.drain_vec(..);
        assert_eq!(
            timestamps,
            vec![0, 1, 3, 6, 10, 15, 21, 28, 36, 45, i32::MAX]
        )
    }

    /// Check epochs get removed if a new assign_timestamps is added (except for MAX)
    #[test]
    fn test_epochs_get_removed() {
        let time_collector = VecSink::new();

        let rt = get_test_rt(|provider| {
            let time_collector = time_collector.clone();
            let (stream, _late) = provider
                .new_stream()
                .source(
                    "source",
                    StatelessSource::new(SingleIteratorSource::new(0..10)),
                )
                .generate_epochs("monotonic-epoch", |msg, _| Some(msg.timestamp));

            // this should remove epochs
            let (stream, _) = stream
                .assign_timestamps("ts-as-i32", |x| x.timestamp as i32)
                .generate_epochs("no-epochs", |_x, _y| None);

            stream.then(epoch_collector("get-epoch", time_collector));
        });

        rt.execute().unwrap();

        let timestamps: Vec<i32> = time_collector.drain_vec(..);
        // only max epoch should have gone through
        assert_eq!(timestamps, vec![i32::MAX])
    }

    /// Check the epoch is issued AFTER the data message given to the
    /// epoch generator
    #[test]
    fn test_epoch_issued_after_message() {
        let collector = VecSink::new();

        let rt = get_test_rt(|provider| {
            let collector = collector.clone();
            let (ontime, _late) = provider
                .new_stream()
                .source(
                    "source",
                    StatelessSource::new(SingleIteratorSource::new(1..4)),
                )
                .assign_timestamps("value-as-ts", |x| x.value)
                .generate_epochs("monotonic", |msg, _epoch| Some(msg.timestamp));

            ontime.then(Operator::direct(
                "collect-msgs".into(),
                async move |input: &mut Input<(NoKey, i32, i32)>,
                            out: &mut Output<(NoKey, i32, i32)>,
                            _: &mut OperatorContext| {
                    match input.recv().await {
                        // encode epoch to -T
                        Message::Data(d) => {
                            collector.give(d.timestamp);
                            out.send(Message::Data(d)).await
                        }
                        Message::Epoch(e) => {
                            collector.give(-e);
                            out.send(Message::Epoch(e)).await
                        }
                        x => out.send(x).await,
                    };
                },
            ));
        });

        rt.execute().unwrap();

        assert_eq!(
            collector.drain_vec(..),
            vec![1, -1, 2, -2, 3, -3, -i32::MAX]
        )
    }

    /// Test late messages are placed into the late stream
    #[test]
    fn test_late_message_into_late_stream() {
        let collector_ontime = VecSink::new();
        let collector_late = VecSink::new();

        let rt = get_test_rt(|provider| {
            let (ontime, late) = provider
                .new_stream()
                .source(
                    "source",
                    StatelessSource::new(SingleIteratorSource::new((5..10).chain(0..5))),
                )
                .assign_timestamps("value-ts", |x| x.value)
                .generate_epochs("monotonic", |msg, _epoch| Some(msg.timestamp));

            ontime.sink("sink-ontime", StatelessSink::new(collector_ontime.clone()));
            late.sink("sink-late", StatelessSink::new(collector_late.clone()));
        });
        rt.execute().unwrap();

        assert_eq!(
            collector_ontime
                .into_iter()
                .map(|x| (x.timestamp, x.value))
                .collect_vec(),
            (5..10).map(|x| (x, x)).collect_vec()
        );
        assert_eq!(
            collector_late.into_iter().map(|x| x.value).collect_vec(),
            (0..5).collect_vec()
        );
    }

    /// test we ignore None epoch or smaller epoch
    #[test]
    fn test_ignore_none_or_smaller_epoch() {
        let collector_ontime = VecSink::new();
        let rt = get_test_rt(|provider| {
            let collector_ontime = collector_ontime.clone();
            let (ontime, _) = provider
                .new_stream()
                .source(
                    "source",
                    StatelessSource::new(SingleIteratorSource::new(0..6)),
                )
                .generate_epochs("out-of-order", |msg, _epoch| {
                    match msg.timestamp {
                        3 => Some(2), // should be ignrored
                        1 => None,    // this too
                        x => Some(x),
                    }
                });
            ontime.then(epoch_collector("get-epoch", collector_ontime));
        });
        rt.execute().unwrap();
        let epochs = collector_ontime.drain_vec(..);
        assert_eq!(epochs, vec![0, 2, 4, 5, usize::MAX])
    }
}
