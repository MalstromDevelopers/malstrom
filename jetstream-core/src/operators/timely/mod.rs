use crate::stream::jetstream::JetStreamBuilder;
use crate::stream::operator::OperatorBuilder;
use crate::time::{MaybeTime, NoTime};
use crate::{Data, DataMessage, MaybeKey, Message};

mod from_data;
mod inspect_frontier;
mod needs_epochs;
mod periodic;
mod util;
pub use self::from_data::GenerateEpochs;
pub use self::inspect_frontier::InspectFrontier;
pub use self::needs_epochs::NeedsEpochs;
pub use self::periodic::PeriodicEpochs;

#[derive(Clone)]
enum OnTimeLate<V> {
    OnTime(V),
    Late(V),
}

pub trait TimelyStream<K, V, T> {
    /// Assigns a new timestamp to every message.
    /// NOTE: Any Epochs arriving at this operator are dropped
    fn assign_timestamps<TO: MaybeTime>(
        self,
        assigner: impl FnMut(&DataMessage<K, V, T>) -> TO + 'static,
    ) -> NeedsEpochs<K, V, TO>;

    fn assign_and_convert<TO: MaybeTime>(
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
    T: MaybeTime,
{
    fn assign_timestamps<TO: MaybeTime>(
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
        NeedsEpochs(self.then(operator))
    }

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

    fn assign_and_convert<TO: MaybeTime>(
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
        operators::{sink::Sink, source::Source},
        stream::operator::OperatorBuilder,
        test::{get_test_configs, get_test_stream, VecCollector},
        Message, NoKey,
    };
    use itertools::Itertools;

    use super::*;

    /// Check that the assigner assigns a timestamp to every record
    #[test]
    fn test_timestamp_gets_assigned() {
        let [config] = get_test_configs();
        let (mut worker, stream) = get_test_stream();
        let collector = VecCollector::new();

        let (ontime, late) = stream
            .source(0..10)
            .assign_timestamps(|x| x.value * 2)
            .generate_epochs(&mut worker, |_x, _y| None);
        let ontime = ontime.sink(collector.clone());

        worker.add_stream(ontime);
        worker.add_stream(late);

        let mut runtime = worker.build(config).unwrap();
        while collector.len() < 10 {
            runtime.step()
        }
        let timestamps = collector
            .drain_vec(..)
            .into_iter()
            .map(|x| x.timestamp)
            .collect_vec();

        assert_eq!((0..10).map(|x| x * 2).collect_vec(), timestamps)
    }

    /// check epochs get issued according to the given function
    #[test]
    fn test_epoch_gets_issued() {
        let [config] = get_test_configs();
        let (mut worker, stream) = get_test_stream();

        let collector = VecCollector::new();
        let late_collector = VecCollector::new();

        let (stream, late) = stream
            .source(0..10)
            .assign_timestamps(|x| x.value)
            .generate_epochs(&mut worker, |msg, epoch| {
                Some(msg.timestamp + epoch.unwrap_or(0))
            });
        let (stream, frontier) = stream.inspect_frontier();
        let stream = stream.sink(collector.clone());
        let late = late.sink(late_collector.clone());

        worker.add_stream(stream);
        worker.add_stream(late);
        let mut runtime = worker.build(config).unwrap();

        let mut timestamps: Vec<i32> = vec![0];
        while collector.len() + late_collector.len() < 10 {
            runtime.step();
            if let Some(i) = frontier.get_time() {
                if i != *timestamps.last().unwrap() {
                    timestamps.push(i)
                }
            }
        }

        assert_eq!(timestamps, vec![0, 1, 3, 6, 10, 15, 21, 28, 36, 45])
    }

    /// check epochs get removed if timestamps are reassigned
    #[test]
    fn test_epoch_gets_removed() {
        let [config] = get_test_configs();
        let (mut worker, stream) = get_test_stream();

        let collector = VecCollector::new();
        let (stream, late) = stream
            .source(0..10)
            .assign_timestamps(|x| x.value)
            .generate_epochs(&mut worker, |msg, _| Some(msg.timestamp));

        // this should remove epochs
        let stream = stream.remove_timestamps();

        let time_collector = VecCollector::new();
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

        worker.add_stream(stream);
        worker.add_stream(late);
        let mut runtime = worker.build(config).unwrap();

        while collector.len() < 10 {
            runtime.step();
        }
        let timestamps: Vec<NoTime> = time_collector.drain_vec(..);

        assert_eq!(timestamps.len(), 0)
    }

    /// Check epochs get removed if a new epoch generator is added
    #[test]
    fn test_epoch_gets_removed_gen() {
        let [config] = get_test_configs();
        let (mut worker, stream) = get_test_stream();

        let collector = VecCollector::new();
        let (stream, late) = stream
            .source(0..10)
            .assign_timestamps(|x| x.value)
            .generate_epochs(&mut worker, |msg, _| Some(msg.timestamp));
        // this should remove epochs
        let stream = stream.generate_epochs(&mut worker, |_x, _y| None).0;

        let time_collector = VecCollector::new();
        let collector_moved = time_collector.clone();
        let stream = stream.then(OperatorBuilder::direct(move |input, output, _| {
            match input.recv() {
                Some(Message::Epoch(t)) => {
                    collector_moved.give(t);
                }
                Some(x) => output.send(x),
                None => (),
            };
        }));

        let stream = stream.sink(collector.clone());

        worker.add_stream(stream);
        worker.add_stream(late);
        let mut runtime = worker.build(config).unwrap();

        while collector.len() < 10 {
            runtime.step();
        }
        let timestamps: Vec<i32> = time_collector.drain_vec(..);
        assert_eq!(timestamps.len(), 0)
    }

    /// Check the epoch is issued AFTER the data message given to the
    /// epoch generator
    #[test]
    fn test_epoch_issued_after_message() {
        let [config] = get_test_configs();
        let (mut worker, stream) = get_test_stream();

        let (ontime, late) = stream
            .source(1..4)
            .assign_timestamps(|x| x.value)
            .generate_epochs(&mut worker, |msg, _epoch| Some(msg.timestamp));

        let collector = VecCollector::new();
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
        worker.add_stream(ontime);
        worker.add_stream(late);
        let mut runtime = worker.build(config).unwrap();

        while collector.len() < 6 {
            runtime.step()
        }

        assert_eq!(collector.drain_vec(..), vec![1, -1, 2, -2, 3, -3])
    }

    /// Test late messages are placed into the late stream
    #[test]
    fn test_late_message_into_late_stream() {
        let [config] = get_test_configs();
        let (mut worker, stream) = get_test_stream();

        let collector_ontime = VecCollector::new();
        let collector_late = VecCollector::new();

        let (ontime, late) = stream
            .source((5..10).chain(0..5))
            .assign_timestamps(|x| x.value)
            .generate_epochs(&mut worker, |msg, _epoch| Some(msg.timestamp));

        worker.add_stream(ontime.sink(collector_ontime.clone()));
        worker.add_stream(late.sink(collector_late.clone()));
        let mut runtime = worker.build(config).unwrap();

        while (collector_late.len() < 5) | (collector_ontime.len() < 5) {
            runtime.step()
        }
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
        todo!()
    }

    #[test]
    fn test_periodic_gen_produces_epochs() {
        todo!()
    }

    #[test]
    fn test_periodic_gen_removes_epochs() {
        todo!()
    }
}
