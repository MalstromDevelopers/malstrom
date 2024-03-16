use crate::{
    operators::source::IntoSource,
    stream::operator::{BuildContext, OperatorBuilder, OperatorContext},
    time::{Epoch, NoTime},
    Data, DataMessage, Message, NoData, NoKey,
};

impl<T: IntoIterator<Item = V> + 'static, V, P> IntoSource<NoKey, V, usize, P> for T
where
    V: Data,
{
    fn into_source(self) -> OperatorBuilder<NoKey, NoData, NoTime, NoKey, V, usize, P> {
        let mut inner = self.into_iter().enumerate();

        let mut has_ended = false;
        OperatorBuilder::direct(move |input, output, _ctx| {
            if let Some(x) = inner.next() {
                output.send(Message::Data(DataMessage::new(NoKey, x.1, x.0)));
                output.send(Message::Epoch(Epoch::new(x.0)))
            } else if !has_ended {
                has_ended = true;
                // send a MAX epoch to indicate the stream has ended
                output.send(Message::Epoch(Epoch::new(usize::MAX)))
            }

            if let Some(msg) = input.recv() {
                match msg {
                    Message::Data(_) => (),
                    Message::Epoch(_) => (),
                    Message::AbsBarrier(x) => output.send(Message::AbsBarrier(x)),
                    Message::Load(x) => output.send(Message::Load(x)),
                    Message::ScaleRemoveWorker(x) => output.send(Message::ScaleRemoveWorker(x)),
                    Message::ScaleAddWorker(x) => output.send(Message::ScaleAddWorker(x)),
                    Message::ShutdownMarker(x) => output.send(Message::ShutdownMarker(x)),
                    Message::Interrogate(x) => output.send(Message::Interrogate(x)),
                    Message::Collect(x) => output.send(Message::Collect(x)),
                    Message::Acquire(x) => output.send(Message::Acquire(x)),
                    Message::DropKey(x) => output.send(Message::DropKey(x)),
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{rc::Rc, sync::Mutex};

    use itertools::Itertools;

    use crate::{
        config::Config,
        operators::{
            probe::{DataOrEpoch, Probe},
            sink::Sink,
            source::Source,
        },
        snapshot::NoPersistence,
        stream::jetstream::JetStreamBuilder,
        test::{get_test_configs, get_test_stream, VecCollector},
        Worker,
    };

    use super::*;

    #[test]
    /// The into_iter source should emit the iterator values
    fn test_emits_values() {
        let (mut worker, stream) = get_test_stream();

        let in_data: Vec<i32> = (0..100).collect();
        let collector = VecCollector::new();

        let stream = stream
            // this should work since Vec is into_iter
            .source(in_data)
            .sink(collector.clone());

        worker.add_stream(stream);
        let [conf] = get_test_configs();
        let mut runtime = worker.build(conf).unwrap();

        while collector.len() < 100 {
            runtime.step()
        }

        let c = collector
            .drain_vec(..)
            .into_iter()
            .map(|x| x.value)
            .collect_vec();
        assert_eq!(c, (0..100).collect_vec())
    }

    #[test]
    /// The into_iter source should emit thi index as a a message timestamp,
    /// followed by the index as an epoch
    /// and finally the MAX epoch when the iterator ends
    fn test_emits_time() {
        let (mut worker, stream) = get_test_stream();
        let in_data: Vec<i32> = (0..100).collect();

        // we expect to get the data message with the index first
        // directly followed by the epoch
        // if the timestamp comes from an epoch we will just make
        // it negative as an indicator
        let mut expected: Vec<i128> = (0..in_data.len().try_into().unwrap())
            .flat_map(|i| [i, -i])
            .collect();
        
        // at the end of the iterator a MAX epoch should be sent
        expected.push(-i128::try_from(usize::MAX).unwrap());

        let epoch_collector = VecCollector::new();
        let epoch_collector_out = epoch_collector.clone();

        let stream = stream.source(in_data).probe(move |x| match x {
            DataOrEpoch::Data(t) => epoch_collector.give((*t).try_into().unwrap()),
            DataOrEpoch::Epoch(t) => epoch_collector.give(-i128::try_from(*t).unwrap()),
        });

        worker.add_stream(stream);
        let [conf] = get_test_configs();
        let mut runtime = worker.build(conf).unwrap();

        while epoch_collector_out.len() < expected.len() {
            runtime.step()
        }

        let e = epoch_collector_out.drain_vec(..);
        assert_eq!(e, expected)
    }

}
