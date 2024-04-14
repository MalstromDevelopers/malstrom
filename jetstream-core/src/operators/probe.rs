use crate::channels::selective_broadcast::{Receiver, Sender};
use crate::channels::watch;
use crate::stream::jetstream::JetStreamBuilder;
use crate::stream::operator::OperatorBuilder;
use crate::time::Timestamp;
use crate::{Data, MaybeKey, Message};

/// A container type which encodes whether a timestamp was
///  extracted from a data message or an epoch
pub enum DataOrEpoch<'a, T> {
    Data(&'a T),
    Epoch(&'a T),
}

pub trait ProbeEpoch<K, V, T> {
    fn probe_epoch(self) -> (JetStreamBuilder<K, V, T>, watch::Receiver<Option<T>>);
}

impl<K, V, T> ProbeEpoch<K, V, T> for JetStreamBuilder<K, V, T>
where
    K: MaybeKey,
    V: Data,
    T: Timestamp + Clone,
{
    fn probe_epoch(self) -> (JetStreamBuilder<K, V, T>, watch::Receiver<Option<T>>) {
        let (tx, rx) = watch::channel::<Option<T>>(None);
        let operator = OperatorBuilder::direct(
            move |input: &mut Receiver<K, V, T>, output: &mut Sender<K, V, T>, _ctx| {
                if let Some(x) = input.recv() {
                    match x {
                        Message::Epoch(e) => {
                            tx.send(Some(e.clone()));
                            output.send(Message::Epoch(e))
                        }
                        m => output.send(m),
                    }
                }
            },
        );
        (self.then(operator), rx)
    }
}
