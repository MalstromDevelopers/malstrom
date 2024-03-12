use std::collections::HashMap;

use itertools::Itertools;
use serde::{de::DeserializeOwned, Serialize};

use crate::{
    channels::selective_broadcast::{Receiver, Sender},
    keyed::distributed::DistData,
    snapshot::PersistenceBackend,
    stream::{
        jetstream::JetStreamBuilder,
        operator::{BuildContext, OperatorBuilder, OperatorContext},
    },
    time::Timestamp,
    Data, DataMessage, Key, Message,
};

pub trait StatefulMap<K, VI, T, P> {
    fn stateful_map<VO: Data, S: Default + Serialize + DeserializeOwned + 'static>(
        self,
        mapper: impl FnMut(VI, &mut S) -> VO + 'static,
    ) -> JetStreamBuilder<K, VO, T, P>;
}

fn build_stateful_map<
    K: Key,
    VI,
    T: Clone,
    P: PersistenceBackend,
    VO: Clone,
    S: Default + Serialize + DeserializeOwned,
>(
    context: &BuildContext<P>,
    mut mapper: impl FnMut(VI, &mut S) -> VO + 'static,
) -> impl FnMut(&mut Receiver<K, VI, T, P>, &mut Sender<K, VO, T, P>, &mut OperatorContext) {
    let mut state: HashMap<K, S> = context.load_state().unwrap_or_default();
    move |input: &mut Receiver<K, VI, T, P>, output: &mut Sender<K, VO, T, P>, ctx| {
        let msg = match input.recv() {
            Some(x) => x,
            None => return,
        };
        let mapped: Message<K, VO, T, P> = match msg {
            Message::Data(DataMessage { key, value, time }) => {
                let st = state.entry(key.to_owned()).or_default();
                let mapped = mapper(value, st);
                Message::Data(DataMessage::new(key, mapped, time))
            }
            Message::Interrogate(mut x) => {
                x.add_keys(&(state.keys().map(|k| k.to_owned()).collect_vec()));
                Message::Interrogate(x)
            }
            Message::Collect(mut c) => {
                if let Some(x) = state.get(&c.key) {
                    c.add_state(ctx.operator_id, x);
                }
                Message::Collect(c)
            }
            Message::Acquire(a) => {
                if let Some(st) = a.take_state(&ctx.operator_id) {
                    state.insert(st.0, st.1);
                }
                Message::Acquire(a)
            }
            Message::DropKey(k) => {
                state.remove(&k);
                Message::DropKey(k)
            }
            // necessary to convince Rust it is a different generic type now
            Message::AbsBarrier(b) => Message::AbsBarrier(b),
            Message::Load(l) => {
                state = l.load(ctx.operator_id).unwrap_or_default();
                Message::Load(l)
            }
            Message::ScaleAddWorker(x) => Message::ScaleAddWorker(x),
            Message::ScaleRemoveWorker(x) => Message::ScaleRemoveWorker(x),
            Message::ShutdownMarker(x) => Message::ShutdownMarker(x),
            Message::Epoch(x) => Message::Epoch(x),
        };
        output.send(mapped)
    }
}

impl<K, VI, T, P> StatefulMap<K, VI, T, P> for JetStreamBuilder<K, VI, T, P>
where
    K: Key,
    VI: DistData,
    T: Timestamp,
    P: PersistenceBackend,
{
    fn stateful_map<VO: Data, S: Default + Serialize + DeserializeOwned + 'static>(
        self,
        mapper: impl FnMut(VI, &mut S) -> VO + 'static,
    ) -> JetStreamBuilder<K, VO, T, P> {
        let _state: Option<HashMap<K, S>> = None;

        let op = OperatorBuilder::built_by(move |ctx| build_stateful_map(ctx, mapper));
        self.then(op)
    }
}
