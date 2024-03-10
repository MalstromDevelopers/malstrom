use std::collections::HashMap;

use itertools::Itertools;

use crate::{
    channels::selective_broadcast::{Receiver, Sender}, snapshot::PersistenceBackend, stream::{
        jetstream::JetStreamBuilder,
        operator::StandardOperator,
    }, time::Timestamp, util::panic_early_data, Data, DataMessage, Key, Message
};

pub trait StatefulMap<K, VI, T, P> {
    fn stateful_map<VO: Data, S: Default + 'static>(
        self,
        mapper: impl FnMut(VI, &mut S) -> VO + 'static,
    ) -> JetStreamBuilder<K, VO, T, P>;
}

impl<K, VI, T, P> StatefulMap<K, VI, T, P> for JetStreamBuilder<K, VI, T, P>
where
    K: Key,
    VI: Data,
    T: Timestamp,
    P: PersistenceBackend
{
    fn stateful_map<VO: Data, S: Default + 'static>(
        self,
        mut mapper: impl FnMut(VI, &mut S) -> VO + 'static,
    ) -> JetStreamBuilder<K, VO, T, P> {
        let mut state: Option<HashMap<K, S>> = None;
        
        let op = StandardOperator::new(move |input: &mut Receiver<K, VI, T, P>, output: &mut Sender<K, VO, T, P>, ctx| {
            let msg = match input.recv() {
                Some(x) => x,
                None => return
            };
            let mapped: Message<K, VO, T, P> = match msg {
                Message::Data(DataMessage {key, value, time }) => {
                let st = state.as_mut().unwrap_or_else(panic_early_data).entry(key.to_owned()).or_default();
                let mapped = mapper(value, st);
                Message::Data(DataMessage::new(key, mapped, time))
                }
                Message::Interrogate(mut x) => {
                    if let Some(s) = &state {
                        x.add_keys(&(s.keys().map(|k| k.to_owned()).collect_vec()));
                    };
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
                Message::Load(l) => {
                    state = Some(l.load(ctx.operator_id).unwrap_or_default());
                    Message::Load(l)
                },
                Message::ScaleAddWorker(x) => Message::ScaleAddWorker(x),
                Message::ScaleRemoveWorker(x) => Message::ScaleRemoveWorker(x),
                Message::ShutdownMarker(x) => Message::ShutdownMarker(x),
                Message::Epoch(x) => Message::Epoch(x),
            };
            output.send(mapped)
        });
        self.then(op)
    }
}
