use crate::{
    stream::{JetStreamBuilder, OperatorBuilder},
    types::{Data, DataMessage, MaybeKey, Message, NoTime, Timestamp},
};

use super::NeedsEpochs;

#[derive(Clone)]
pub(super) enum OnTimeLate<V> {
    OnTime(V),
    Late(V),
}

pub trait TimelyStream<K, V, T> {
    /// Assigns a new timestamp to every message.
    /// NOTE: Any Epochs arriving at this operator are dropped
    fn assign_timestamps<TO: Timestamp>(
        self,
        name: &str,
        assigner: impl FnMut(&DataMessage<K, V, T>) -> TO + 'static,
    ) -> NeedsEpochs<K, V, TO>;
}

impl<K, V, T> TimelyStream<K, V, T> for JetStreamBuilder<K, V, T>
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
        operators::{sink::SinkFull, GenerateEpochs, Sink, Source},
        sources::{SingleIteratorSource, StatelessSource},
        stream::OperatorBuilder,
        testing::{get_test_stream, VecSink},
        types::{Message, NoKey},
    };
    use itertools::Itertools;

    use super::*;

    /// Check that the assigner assigns a timestamp to every record
    #[test]
    fn test_timestamp_gets_assigned() {
        let (builder, stream) = get_test_stream();
        let collector = VecSink::new();

        let (ontime, late) = stream
            .source(
                "source",
                StatelessSource::new(SingleIteratorSource::new(0..10)),
            )
            .assign_timestamps("ts-double-value", |x| x.value * 2)
            .generate_epochs("no-epochs", |_x, _y| None);
        let ontime = ontime.sink("sink", collector.clone());

        ontime.finish();
        late.finish();

        builder.build().unwrap().execute();
        let timestamps = collector.into_iter().map(|x| x.timestamp).collect_vec();

        assert_eq!((0..10).map(|x| x * 2).collect_vec(), timestamps)
    }

    /// check epochs get issued according to the given function
    #[test]
    fn test_epoch_gets_issued() {
        let (builder, stream) = get_test_stream();

        let collector = VecSink::new();
        let late_collector = VecSink::new();

        let (stream, late) = stream
            .source(
                "source",
                StatelessSource::new(SingleIteratorSource::new(0..10)),
            )
            .assign_timestamps("ts-from-value", |x| x.value)
            .generate_epochs("add-epoch", |msg, epoch| {
                Some(msg.timestamp + epoch.unwrap_or(0))
            });

        stream.sink_full("sink-ontime", collector.clone()).finish();
        late.sink("sink", late_collector.clone()).finish();

        builder.build().unwrap().execute();

        let timestamps: Vec<i32> = collector
            .into_iter()
            .filter_map(|msg| {
                if let Message::Epoch(e) = msg {
                    Some(e)
                } else {
                    None
                }
            })
            .collect_vec();
        assert_eq!(
            timestamps,
            vec![0, 1, 3, 6, 10, 15, 21, 28, 36, 45, i32::MAX]
        )
    }

    /// Check epochs get removed if a new assign_timestamps is added (except for MAX)
    #[test]
    fn test_epochs_get_removed() {
        let (worker, stream) = get_test_stream();

        let (stream, late) = stream
            .source(
                "source",
                StatelessSource::new(SingleIteratorSource::new(0..10)),
            )
            .generate_epochs("monotonic-epoch", |msg, _| Some(msg.timestamp));

        // this should remove epochs
        let (stream, _) = stream
            .assign_timestamps("ts-as-i32", |x| x.timestamp as i32)
            .generate_epochs("no-epochs", |_x, _y| None);

        let time_collector = VecSink::new();

        stream.sink_full("sink", time_collector.clone()).finish();
        late.finish();
        worker.build().unwrap().execute();

        let timestamps: Vec<i32> = time_collector
            .into_iter()
            .filter_map(|x| match x {
                Message::Epoch(e) => Some(e),
                _ => None,
            })
            .collect();
        // only max epoch should have gone through
        assert_eq!(timestamps, vec![i32::MAX])
    }

    /// Check the epoch is issued AFTER the data message given to the
    /// epoch generator
    #[test]
    fn test_epoch_issued_after_message() {
        let (builder, stream) = get_test_stream();

        let (ontime, late) = stream
            .source(
                "source",
                StatelessSource::new(SingleIteratorSource::new(1..4)),
            )
            .assign_timestamps("value-as-ts", |x| x.value)
            .generate_epochs("monotonic", |msg, _epoch| Some(msg.timestamp));

        let collector = VecSink::new();
        let collector_moved = collector.clone();
        // TODO: use sink-all
        let ontime = ontime.then(OperatorBuilder::direct(
            "collect-msgs",
            move |input: &mut Input<NoKey, i32, i32>, out, _| {
                match input.recv() {
                    // encode epoch to -T
                    Some(Message::Data(d)) => {
                        collector_moved.give(d.timestamp);
                        out.send(Message::Data(d))
                    }
                    Some(Message::Epoch(e)) => {
                        collector_moved.give(-e);
                        out.send(Message::Epoch(e))
                    }
                    Some(x) => out.send(x),
                    None => (),
                };
            },
        ));
        ontime.finish();
        late.finish();
        builder.build().unwrap().execute();

        assert_eq!(
            collector.drain_vec(..),
            vec![1, -1, 2, -2, 3, -3, -i32::MAX]
        )
    }

    /// Test late messages are placed into the late stream
    #[test]
    fn test_late_message_into_late_stream() {
        let (builder, stream) = get_test_stream();

        let collector_ontime = VecSink::new();
        let collector_late = VecSink::new();

        let (ontime, late) = stream
            .source(
                "source",
                StatelessSource::new(SingleIteratorSource::new((5..10).chain(0..5))),
            )
            .assign_timestamps("value-ts", |x| x.value)
            .generate_epochs("monotonic", |msg, _epoch| Some(msg.timestamp));

        ontime
            .sink("sink-ontime", collector_ontime.clone())
            .finish();
        late.sink("sink-late", collector_late.clone()).finish();
        builder.build().unwrap().execute();

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
        let (builder, stream) = get_test_stream();

        let collector_ontime = VecSink::new();

        let (ontime, _) = stream
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
        ontime
            .sink_full("sink-ontime", collector_ontime.clone())
            .finish();
        builder.build().unwrap().execute();
        let epochs = collector_ontime
            .into_iter()
            .filter_map(|msg| {
                if let Message::Epoch(e) = msg {
                    Some(e)
                } else {
                    None
                }
            })
            .collect_vec();
        assert_eq!(epochs, vec![0, 2, 4, 5, usize::MAX])
    }
}
