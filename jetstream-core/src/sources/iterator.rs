use crate::{operators::source::IntoSource, stream::operator::{BuildContext, OperatorBuilder, OperatorContext}, time::{Epoch, NoTime}, Data, DataMessage, Message, NoData, NoKey};

impl<T: IntoIterator<Item = V> + 'static, V, P> IntoSource<NoKey, V, usize, P> for T where V: Data{
    fn into_source(self) -> OperatorBuilder<NoKey, NoData, NoTime, NoKey, V, usize, P> {
        let mut inner = self.into_iter().enumerate();
        OperatorBuilder::direct(move |input, output, _ctx| {
            if let Some(msg) = input.recv() {
                if let Some(x) = inner.next() {
                    output.send(Message::Data(DataMessage::new(NoKey, x.1, x.0)));
                    output.send(Message::Epoch(Epoch::new(x.0)))
                }
                match msg {
                    Message::Data(_) => (),
                    Message::Epoch(_) => (),
                    Message::AbsBarrier(x) =>  output.send(Message::AbsBarrier(x)),
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
    use std::rc::Rc;

    use crate::{operators::{probe::{DataOrEpoch, Probe}, sink::Sink, source::Source}, stream::jetstream::JetStreamBuilder};

    use super::*;
    #[test]
    /// The into_iter source should emit the previously sent index as an epoch
    fn test_emits_epochs() {
        let in_data: Vec<i32> = (0..100).collect();
        let mut epochs: Vec<usize> = Vec::new();
        let mut collected: Rc<Vec<DataMessage<NoKey, i32, usize>>> = Rc::new(Vec::new());
        let stream = JetStreamBuilder::new_test()
        // this should work since Vec is into_iter
        .source(in_data)
        .probe(|x| if let DataOrEpoch::Epoch(e) = x {epochs.push(e.clone())})
        .sink(&mut collected);

    }
}