use std::collections::HashMap;

use itertools::Itertools;

use crate::{
    channels::selective_broadcast::{Receiver, Sender}, frontier::FrontierHandle, snapshot::{barrier::BarrierData, PersistenceBackend}, stream::{
        jetstream::JetStreamBuilder,
        operator::StandardOperator,
    }, Data, DataMessage, Key, Message
};

pub trait StatefulMap<K, TI, P: PersistenceBackend> {
    fn stateful_map<TO: Data, S: Default + 'static>(
        self,
        mapper: impl FnMut(TI, &mut S) -> TO + 'static,
    ) -> JetStreamBuilder<K, TO, P>;
}

impl<K, TI, P> StatefulMap<K, TI, P> for JetStreamBuilder<K, TI, P>
where
    K: Key,
    TI: Data,
    P: PersistenceBackend,
{
    fn stateful_map<TO: Data, S: Default + 'static>(
        self,
        mut mapper: impl FnMut(TI, &mut S) -> TO + 'static,
    ) -> JetStreamBuilder<K, TO, P> {
        let mut state: HashMap<K, S> = HashMap::new();
        let op = StandardOperator::new(move |input: &mut Receiver<K, TI, P>, output: &mut Sender<K, TO, P>, ctx| {
            let msg = match input.recv() {
                Some(x) => x,
                None => return
            };
            let mapped = match msg {
                Message::Data(DataMessage { time, key, value }) => {
                let state = state.entry(key.to_owned()).or_default();
                let mapped = mapper(value, state);
                Message::Data(DataMessage::new(time, key, mapped))
                }
                // key messages may not cross key region boundaries
                Message::Interrogate(mut x) => {
                    x.add_keys(&(state.keys().map(|k| k.to_owned()).collect_vec()));
                    Message::Interrogate(x)
                },
                Message::Collect(x) => {
                    // x.add_state(ctx.operator_id, state.get(&x.key))
                    todo!()
                },
                Message::Acquire(x) => todo!(),
                Message::DropKey(x) => todo!(),
                // necessary to convince Rust it is a different generic type now
                Message::AbsBarrier(b) => Message::AbsBarrier(b),
                Message::Load(l) => Message::Load(l),
                Message::ScaleAddWorker(x) => Message::ScaleAddWorker(x),
                Message::ScaleRemoveWorker(x) => Message::ScaleRemoveWorker(x),
                Message::ShutdownMarker(x) => Message::ShutdownMarker(x),
            };
            output.send(mapped)
        });
        self.then(op)
    }
}
