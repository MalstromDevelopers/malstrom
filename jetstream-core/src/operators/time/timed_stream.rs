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
        assigner: impl FnMut(&DataMessage<K, V, T>) -> TO + 'static,
    ) -> NeedsEpochs<K, V, TO>;

    fn assign_timestamps_and_convert_epochs<TO: Timestamp>(
        self,
        assigner: impl FnMut(&DataMessage<K, V, T>) -> TO + 'static,
        converter: impl FnMut(T) -> Option<TO> + 'static,
    ) -> JetStreamBuilder<K, V, TO>;

    /// Turns a timestamped stream into an untimestamped stream by
    /// removing all message timestamps and epochs    
    fn remove_timestamps(self) -> JetStreamBuilder<K, V, NoTime>;
}

impl<K, V, T> TimelyStream<K, V, T> for JetStreamBuilder<K, V, T>
where
    K: MaybeKey,
    V: Data,
    T: Timestamp,
{
    fn assign_timestamps<TO: Timestamp>(
        self,
        mut assigner: impl FnMut(&DataMessage<K, V, T>) -> TO + 'static,
    ) -> NeedsEpochs<K, V, TO> {
        let operator = OperatorBuilder::direct(move |input, output, _| {
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
                    Message::DropKey(k) => output.send(Message::DropKey(k)),
                    Message::AbsBarrier(b) => output.send(Message::AbsBarrier(b)),
                    // Message::Load(l) => output.send(Message::Load(l)),
                    Message::Rescale(x) => output.send(Message::Rescale(x)),
                    Message::ShutdownMarker(x) => output.send(Message::ShutdownMarker(x)),
                }
            }
        });
        NeedsEpochs(self.then(operator))
    }

    /// TODO This should only be pub(crate) at most
    fn remove_timestamps(self) -> JetStreamBuilder<K, V, NoTime> {
        let operator = OperatorBuilder::direct(|input, output, _| {
            if let Some(msg) = input.recv() {
                match msg {
                    Message::Data(DataMessage {
                        key,
                        value,
                        timestamp: _,
                    }) => output.send(Message::Data(DataMessage::new(key, value, NoTime))),
                    Message::Epoch(_) => (),
                    Message::Interrogate(x) => output.send(Message::Interrogate(x)),
                    Message::Collect(c) => output.send(Message::Collect(c)),
                    Message::Acquire(a) => output.send(Message::Acquire(a)),
                    Message::DropKey(k) => output.send(Message::DropKey(k)),
                    Message::AbsBarrier(b) => output.send(Message::AbsBarrier(b)),
                    // Message::Load(l) => output.send(Message::Load(l)),
                    Message::Rescale(x) => output.send(Message::Rescale(x)),
                    Message::ShutdownMarker(x) => output.send(Message::ShutdownMarker(x)),
                }
            }
        });
        self.then(operator)
    }

    fn assign_timestamps_and_convert_epochs<TO: Timestamp>(
        self,
        mut assigner: impl FnMut(&DataMessage<K, V, T>) -> TO + 'static,
        mut converter: impl FnMut(T) -> Option<TO> + 'static,
    ) -> JetStreamBuilder<K, V, TO> {
        let operator = OperatorBuilder::direct(move |input, output, _| {
            if let Some(msg) = input.recv() {
                match msg {
                    Message::Data(d) => {
                        let timestamp = assigner(&d);
                        let new = DataMessage::new(d.key, d.value, timestamp);
                        output.send(Message::Data(new))
                    }
                    Message::Epoch(e) => {
                        if let Some(new_e) = converter(e) {
                            output.send(Message::Epoch(new_e))
                        }
                    }
                    Message::Interrogate(x) => output.send(Message::Interrogate(x)),
                    Message::Collect(c) => output.send(Message::Collect(c)),
                    Message::Acquire(a) => output.send(Message::Acquire(a)),
                    Message::DropKey(k) => output.send(Message::DropKey(k)),
                    Message::AbsBarrier(b) => output.send(Message::AbsBarrier(b)),
                    // Message::Load(l) => output.send(Message::Load(l)),
                    Message::Rescale(x) => output.send(Message::Rescale(x)),
                    Message::ShutdownMarker(x) => output.send(Message::ShutdownMarker(x)),
                }
            }
        });
        self.then(operator)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        channels::selective_broadcast::Receiver,
        operators::{sink::SinkFull, GenerateEpochs, Sink, Source},
        sources::SingleIteratorSource,
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
            .source(SingleIteratorSource::new(0..10))
            .assign_timestamps(|x| x.value * 2)
            .generate_epochs(|_x, _y| None);
        let ontime = ontime.sink(collector.clone());

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
            .source(SingleIteratorSource::new(0..10))
            .assign_timestamps(|x| x.value)
            .generate_epochs(|msg, epoch| Some(msg.timestamp + epoch.unwrap_or(0)));

        stream.sink_full(collector.clone()).finish();
        late.sink(late_collector.clone()).finish();

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

    /// check epochs get removed if timestamps are reassigned
    #[test]
    fn test_epoch_gets_removed() {
        let (worker, stream) = get_test_stream();

        let collector = VecSink::new();
        let (stream, late) = stream
            .source(SingleIteratorSource::new(0..10))
            .assign_timestamps(|x| x.value)
            .generate_epochs(|msg, _| Some(msg.timestamp));

        // this should remove epochs
        let stream = stream.remove_timestamps();

        let time_collector = VecSink::new();
        let collector_moved = time_collector.clone();
        let stream = stream.then(OperatorBuilder::direct(move |input, out, _| {
            match input.recv() {
                Some(Message::Epoch(t)) => {
                    collector_moved.give(t);
                }
                Some(x) => out.send(x),
                None => (),
            };
        }));

        let stream = stream.sink(collector.clone());

        stream.finish();
        late.finish();
        let mut runtime = worker.build().unwrap();

        while collector.len() < 10 {
            runtime.step();
        }
        let timestamps: Vec<NoTime> = time_collector.drain_vec(..);

        assert_eq!(timestamps.len(), 0)
    }

    /// Check epochs get removed if a new assign_timestamps is added (except for MAX)
    #[test]
    fn test_epochs_get_removed() {
        let (worker, stream) = get_test_stream();

        let (stream, late) = stream
            .source(SingleIteratorSource::new(0..10))
            .generate_epochs(|msg, _| Some(msg.timestamp));

        // this should remove epochs
        let (stream, _) = stream.assign_timestamps(|x| x.timestamp as i32).generate_epochs(|_x, _y| None);

        let time_collector = VecSink::new();

        stream.sink_full(time_collector.clone()).finish();
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
            .source(SingleIteratorSource::new(1..4))
            .assign_timestamps(|x| x.value)
            .generate_epochs(|msg, _epoch| Some(msg.timestamp));

        let collector = VecSink::new();
        let collector_moved = collector.clone();
        let ontime = ontime.then(OperatorBuilder::direct(
            move |input: &mut Receiver<NoKey, i32, i32>, out, _| {
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

        assert_eq!(collector.drain_vec(..), vec![1, -1, 2, -2, 3, -3, -i32::MAX])
    }

    /// Test late messages are placed into the late stream
    #[test]
    fn test_late_message_into_late_stream() {
        let (builder, stream) = get_test_stream();

        let collector_ontime = VecSink::new();
        let collector_late = VecSink::new();

        let (ontime, late) = stream
            .source(SingleIteratorSource::new((5..10).chain(0..5)))
            .assign_timestamps(|x| x.value)
            .generate_epochs(|msg, _epoch| Some(msg.timestamp));

        ontime.sink(collector_ontime.clone()).finish();
        late.sink(collector_late.clone()).finish();
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
            .source(SingleIteratorSource::new(0..6))
            .generate_epochs(|msg, _epoch| {
                match msg.timestamp {
                    3 => Some(2), // should be ignrored
                    1 => None, // this too
                    x => Some(x)
                }
            });
        ontime.sink_full(collector_ontime.clone()).finish();
        builder.build().unwrap().execute();
        let epochs = collector_ontime.into_iter().filter_map(|msg| if let Message::Epoch(e) = msg {Some(e)} else { None}).collect_vec();
        assert_eq!(epochs, vec![0, 2, 4, 5, usize::MAX])
    }
}
