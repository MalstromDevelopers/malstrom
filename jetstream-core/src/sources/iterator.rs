use crate::{
    operators::source::IntoSource,
    stream::operator::OperatorBuilder,
    time::{Epoch, NoTime},
    Data, DataMessage, Message, NoData, NoKey,
};

impl<T: IntoIterator<Item = V> + 'static, V, P> IntoSource<NoKey, V, NoTime, P> for T
where
    V: Data,
{
    fn into_source(self) -> OperatorBuilder<NoKey, NoData, NoTime, NoKey, V, NoTime, P> {
        let mut inner = self.into_iter();
        OperatorBuilder::direct(move |input, output, ctx| {
            if ctx.worker_id == 0 {
                if let Some(x) = inner.next() {
                    output.send(Message::Data(DataMessage::new(NoKey, x, NoTime)));
                }
            }

            if let Some(msg) = input.recv() {
                match msg {
                    Message::Data(_) => (),
                    Message::Epoch(_) => (),
                    Message::AbsBarrier(x) => output.send(Message::AbsBarrier(x)),
                    // Message::Load(x) => output.send(Message::Load(x)),
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

    use itertools::Itertools;

    use crate::{
        operators::{
            probe::{DataOrEpoch, ProbeEpoch},
            sink::Sink,
            source::Source,
        },
        snapshot::NoPersistence,
        test::{get_test_configs, get_test_stream, VecCollector},
        Worker,
    };

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
    /// It should only emit records on worker 0 to avoid duplicats
    fn test_emits_only_on_worker_0() {
        let [config0, config1] = get_test_configs();
        let (mut worker, stream) = get_test_stream();

        let in_data: Vec<i32> = (0..100).collect();
        let collector = VecCollector::new();

        let stream = stream.source(in_data).sink(collector.clone());

        // we need to start up a new thread with another worker
        // since the .build method will try to establish communication
        let _ = std::thread::spawn(move || {
            let worker = Worker::<NoPersistence>::new(|| false);
            worker.build(config0).unwrap();
        });

        worker.add_stream(stream);
        let mut runtime = worker.build(config1).unwrap();

        for _ in 0..100 {
            runtime.step()
        }
        let c = collector
            .drain_vec(..)
            .into_iter()
            .map(|x| x.value)
            .collect_vec();
        assert_eq!(c, Vec::<i32>::new())
    }
}
