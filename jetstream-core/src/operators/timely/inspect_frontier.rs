use super::super::stateless_op::StatelessOp;
use crate::channels::selective_broadcast::Receiver;
use crate::stream::jetstream::JetStreamBuilder;
use crate::stream::operator::OperatorBuilder;
use crate::stream::operator_trait::OperatorTrait;
use crate::time::{MaybeTime, Timestamp};
use crate::channels::watch;
use crate::{Data, DataMessage, MaybeData, MaybeKey, Message};

pub struct FrontierHandle<T> {
    current_time: watch::Receiver<Option<T>>
}
impl<T> FrontierHandle<T> where T: Timestamp{
    pub fn get_time(&self) -> Option<T> {
        self.current_time.read()
    }
}

pub trait InspectFrontier<K, V, T> {
    fn inspect_frontier(self) -> (JetStreamBuilder<K, V, T>, FrontierHandle<T>);
}

impl<K, V, T> InspectFrontier<K, V, T> for JetStreamBuilder<K, V, T>
where
    K: MaybeKey,
    V: MaybeData,
    T: Timestamp,
{
    fn inspect_frontier(self) -> (JetStreamBuilder<K, V, T>, FrontierHandle<T>) {
        let (tx, rx) = watch::channel::<Option<T>>(None);
        let handle = FrontierHandle{current_time: rx};
        let stream = self.then(OperatorBuilder::direct(move |input: &mut Receiver<K, V, T>, output, _| {
            if let Some(msg) = input.recv() {
                match msg {
                    Message::Epoch(e) => {
                        tx.send(Some(e.clone()));
                        output.send(Message::Epoch(e))
                    }
                    x => output.send(x)
                }
            };
        }));
        (stream, handle)
    }
}
