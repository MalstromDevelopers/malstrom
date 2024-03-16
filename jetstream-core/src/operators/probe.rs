use super::stateless_op::StatelessOp;
use crate::stream::jetstream::JetStreamBuilder;
use crate::stream::operator::OperatorBuilder;
use crate::time::{MaybeTime, Timestamp};
use crate::{Data, MaybeKey, Message};

/// A container type which encodes whether a timestamp was
///  extracted from a data message or an epoch
pub enum DataOrEpoch<'a, T> {
    Data(&'a T),
    Epoch(&'a T),
}

pub trait Probe<K, V, T, P> {
    fn probe(
        self,
        probe: impl FnMut(DataOrEpoch<T>) -> () + 'static,
    ) -> JetStreamBuilder<K, V, T, P>;
}

impl<K, V, T, P> Probe<K, V, T, P> for JetStreamBuilder<K, V, T, P>
where
    K: MaybeKey,
    V: Data,
    T: Timestamp,
    P: 'static,
{
    fn probe(
        self,
        mut probe: impl FnMut(DataOrEpoch<T>) -> () + 'static,
    ) -> JetStreamBuilder<K, V, T, P> {
        let operator = OperatorBuilder::direct(move |input, output, _ctx| {
            if let Some(x) = input.recv() {
                match x {
                    Message::Data(d) => {
                        probe(DataOrEpoch::Data(&d.timestamp));
                        output.send(Message::Data(d))
                    }
                    Message::Epoch(e) => {
                        probe(DataOrEpoch::Epoch(&e.timestamp));
                        output.send(Message::Epoch(e))
                    }
                    m => output.send(m),
                }
            }
        });
        self.then(operator)
    }
}
