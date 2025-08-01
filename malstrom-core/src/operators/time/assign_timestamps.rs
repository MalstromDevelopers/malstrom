use crate::{
    operators::sealed::Sealed,
    stream::{OperatorBuilder, StreamBuilder},
    types::{Data, DataMessage, MaybeKey, Message, Timestamp},
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
pub trait AssignTimestamps<K, V, T>: Sealed {
    /// Assigns a new timestamp to every message.
    /// NOTE: Any Epochs arriving at this operator are dropped with the exception
    /// of the `MAX` epoch. See [Timestamp::MAX]
    fn assign_timestamps<TO: Timestamp>(
        self,
        name: &str,
        assigner: impl FnMut(&DataMessage<K, V, T>) -> TO + 'static,
    ) -> NeedsEpochs<K, V, TO>;
}

impl<K, V, T> AssignTimestamps<K, V, T> for StreamBuilder<K, V, T>
where
    K: MaybeKey,
    V: Data,
    T: Timestamp,
{
    fn assign_timestamps<TO: Timestamp>(
        self,
        name: &str,
        mut assigner: impl FnMut(&DataMessage<K, V, T>) -> TO + 'static,
    ) -> NeedsEpochs<K, V, TO> {
        let operator = OperatorBuilder::direct(name, move |input, output, _| {
            if let Some(msg) = input.recv() {
                match msg {
                    Message::Data(d) => {
                        let timestamp = assigner(&d);
                        let new = DataMessage::new(d.key, d.value, timestamp);
                        output.send(Message::Data(new))
                    }
                    Message::Epoch(e) => {
                        if e == T::MAX {
                            output.send(Message::Epoch(TO::MAX))
                        }
                    }
                    Message::Interrogate(x) => output.send(Message::Interrogate(x)),
                    Message::Collect(c) => output.send(Message::Collect(c)),
                    Message::Acquire(a) => output.send(Message::Acquire(a)),
                    Message::AbsBarrier(b) => output.send(Message::AbsBarrier(b)),
                    // Message::Load(l) => output.send(Message::Load(l)),
                    Message::Rescale(x) => output.send(Message::Rescale(x)),
                    Message::SuspendMarker(x) => output.send(Message::SuspendMarker(x)),
                }
            }
        });
        NeedsEpochs(self.then(operator))
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        channels::operator_io::Input,
        operators::{GenerateEpochs, Sink, Source},
        sinks::StatelessSink,
        sources::{SingleIteratorSource, StatelessSource},
        stream::OperatorBuilder,
        testing::get_test_rt,
        testing::VecSink,
        types::{MaybeData, MaybeTime, Message, NoKey},
    };
    use itertools::Itertools;

    use super::*;

    fn epoch_collector<K, V, T>(
        name: &str,
        collector: VecSink<T>,
    ) -> OperatorBuilder<K, V, T, K, V, T>
    where
        K: MaybeKey,
        V: MaybeData,
        T: MaybeTime + Clone,
    {
        OperatorBuilder::direct(name, move |input: &mut Input<K, V, T>, output, _| {
            if let Some(msg) = input.recv() {
                match msg {
                    Message::Epoch(e) => {
                        collector.give(e.clone());
                        output.send(Message::Epoch(e));
                    }
                    x => output.send(x),
                }
            };
        })
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

            ontime.then(OperatorBuilder::direct(
                "collect-msgs",
                move |input: &mut Input<NoKey, i32, i32>, out, _| {
                    match input.recv() {
                        // encode epoch to -T
                        Some(Message::Data(d)) => {
                            collector.give(d.timestamp);
                            out.send(Message::Data(d))
                        }
                        Some(Message::Epoch(e)) => {
                            collector.give(-e);
                            out.send(Message::Epoch(e))
                        }
                        Some(x) => out.send(x),
                        None => (),
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
