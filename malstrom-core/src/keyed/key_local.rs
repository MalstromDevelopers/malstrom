use std::marker::PhantomData;

use crate::channels::operator_io::{Input, Output};

use crate::stream::{Logic, Malstrom, Operator, StreamBuilder};
use crate::types::{Data, DataMessage, Key, Kvt, MaybeKey, MaybeTime, Message};

/// Create a keyed stream **without** distributing messages.
pub trait KeyLocal<M: Kvt, N: Kvt> {
    /// Turn a stream into a keyed stream and **do not** distribute
    /// messages across workers.
    /// # ⚠️ Warning:
    /// The keyed stream created by this function **does not**
    /// redistribute state when the local worker is shut down.
    /// If the worker gets de-scheduled all state is potentially lost.
    /// To have the state moved to a different worker in this case, use
    /// `key_distribute`.
    fn key_local<F: Fn(&DataMessage<M>) -> N::Key + 'static>(
        self,
        name: impl Into<String>,
        key_func: F,
    ) -> StreamBuilder<N>;
}

impl<M, N, X> KeyLocal<M, N> for X
where
    X: Malstrom<M>,
    M: Kvt,
    N: Kvt<Value = M::Value, Timestamp = M::Timestamp>,
{
    fn key_local<F: Fn(&DataMessage<M>) -> N::Key + 'static>(
        self,
        name: impl Into<String>,
        key_func: F,
    ) -> StreamBuilder<N> {
        let op = Operator::direct(
            name.into(),
            KeyLocalImpl {
                key_func,
            },
        );
        self.then(op)
    }
}

struct KeyLocalImpl<F> {
    key_func: F,
}

impl<F, K, M, N> Logic<M, N> for KeyLocalImpl<F>
where
    M: Kvt,
    N: Kvt<Key = K, Value = M::Value, Timestamp = M::Timestamp>,
    F: Fn(&DataMessage<M>) -> K + 'static,
{
    async fn apply(
        &mut self,
        input: &mut Input<M>,
        output: &mut Output<N>,
        _ctx: &mut crate::stream::OperatorContext<'_>,
    ) {
        match input.recv() {
            Some(Message::Data(d)) => {
                let new_key = (self.key_func)(&d);
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
            Some(Message::Rescale(x)) => output.send(Message::Rescale(x)),
            Some(Message::SuspendMarker(x)) => output.send(Message::SuspendMarker(x)),
            Some(Message::Epoch(x)) => output.send(Message::Epoch(x)),
            None => (),
        }
    }
}
