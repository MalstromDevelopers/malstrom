use crate::channels::operator_io::{Input, Output};

use crate::stream::{JetStreamBuilder, OperatorBuilder};
use crate::types::{Data, DataMessage, Key, MaybeKey, Message, Timestamp};

pub trait KeyLocal<X, K: Key, V, T> {
    /// Turn a stream into a keyed stream and **do not** distribute
    /// messages across workers.
    /// # ⚠️ Warning:
    /// The keyed stream created by this function **does not**
    /// redistribute state when the local worker is shut down.
    /// If the worker gets de-scheduled all state is potentially lost.
    /// To have the state moved to a different worker in this case, use
    /// `key_distribute`.
    fn key_local(
        self,
        name: &str,

        key_func: impl Fn(&DataMessage<X, V, T>) -> K + 'static,
    ) -> JetStreamBuilder<K, V, T>;
}

impl<X, K, V, T> KeyLocal<X, K, V, T> for JetStreamBuilder<X, V, T>
where
    X: MaybeKey,
    K: Key,
    V: Data,
    T: Timestamp,
{
    fn key_local(
        self,
        name: &str,

        key_func: impl Fn(&DataMessage<X, V, T>) -> K + 'static,
    ) -> JetStreamBuilder<K, V, T> {
        let op = OperatorBuilder::direct(
            name,
            move |input: &mut Input<X, V, T>, output: &mut Output<K, V, T>, _ctx| {
                match input.recv() {
                    Some(Message::Data(d)) => {
                        let new_key = key_func(&d);
                        let new_msg = DataMessage {
                            timestamp: d.timestamp,
                            key: new_key,
                            value: d.value,
                        };
                        output.send(Message::Data(new_msg))
                    }
                    // key messages may not cross key region boundaries
                    Some(Message::Interrogate(_)) => (),
                    Some(Message::Collect(_)) => (),
                    Some(Message::Acquire(_)) => (),
                    // necessary to convince Rust it is a different generic type now
                    Some(Message::AbsBarrier(b)) => output.send(Message::AbsBarrier(b)),
                    // Some(Message::Load(l)) => output.send(Message::Load(l)),
                    Some(Message::Rescale(x)) => output.send(Message::Rescale(x)),
                    Some(Message::SuspendMarker(x)) => output.send(Message::SuspendMarker(x)),
                    Some(Message::Epoch(x)) => output.send(Message::Epoch(x)),
                    None => (),
                }
            },
        );
        self.then(op)
    }
}
