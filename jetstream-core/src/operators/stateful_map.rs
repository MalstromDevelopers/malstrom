use std::collections::HashMap;

use itertools::Itertools;
use metrics::gauge;
use serde::{de::DeserializeOwned, Serialize};
use tracing::{event, Level};

use crate::{
    channels::selective_broadcast::{Receiver, Sender},
    keyed::distributed::DistData,
    snapshot::PersistenceBackend,
    stream::{
        jetstream::JetStreamBuilder,
        operator::{BuildContext, OperatorBuilder, OperatorContext},
    },
    time::{MaybeTime, Timestamp},
    Data, DataMessage, Key, Message,
};

pub trait StatefulMap<K, VI, T> {
    /// Transforms data utilizing some managed state.
    ///
    /// This operator will apply a transforming function to every message.
    /// The function gets ownership of the state belonging to that message's
    /// key and can either return a new state or `None` to indicate, that
    /// the state for this key need not be retained.
    ///
    /// Any state can be used as long as it implements the `Default`, `Serialize``
    /// and `DeserializeOwned` traits.
    /// A default is required, to create the inital state, when the state for a key
    /// does not yet exist (or has been dropped).
    /// The `Serialize` and `Deserialize` traits are required to make the state
    /// distributable on cluster resizes.
    ///
    /// # Examples
    ///
    /// This dataflow creates batches of 5 messages each
    ///
    /// ```
    /// stream: JetStreamBuilder<usize, String, NoTime, NoPersistence>
    ///
    /// stream
    ///     .stateful_map(
    ///         |value, mut state: Vec<String>| {
    ///             if state.len() == 5 {
    ///                 (Some(state), None)
    ///             } else {
    ///                 state.push(value);
    ///                 (None, state)        
    ///             }
    ///         }
    ///     )
    ///    .filter_map(|value| value)
    /// ```
    fn stateful_map<VO: Data, S: Default + Serialize + DeserializeOwned + 'static>(
        self,
        mapper: impl FnMut(&K, VI, S) -> (VO, Option<S>) + 'static,
    ) -> JetStreamBuilder<K, VO, T>;
}

fn build_stateful_map<
    K: Key + Serialize + DeserializeOwned,
    VI,
    T: MaybeTime,
    VO: Clone,
    S: Default + Serialize + DeserializeOwned,
>(
    context: &BuildContext,
    mut mapper: impl FnMut(&K, VI, S) -> (VO, Option<S>) + 'static,
) -> impl FnMut(&mut Receiver<K, VI, T>, &mut Sender<K, VO, T>, &mut OperatorContext) {
    let mut state: HashMap<K, S> = context.load_state().unwrap_or_default();
    let state_size = gauge!("{}.stateful_map.state_size", "label" => format!("{}", context.label));
        
    move |input: &mut Receiver<K, VI, T>, output: &mut Sender<K, VO, T>, ctx| {
        let msg = match input.recv() {
            Some(x) => x,
            None => return,
        };
        let mapped: Message<K, VO, T> = match msg {
            Message::Data(DataMessage {
                key,
                value,
                timestamp: time,
            }) => {
                event!(Level::INFO, state_key_space = state.len());
                let st = state.remove(&key).unwrap_or_default();
                let (mapped, mut new_state) = mapper(&key, value, st);
                if let Some(n) = new_state.take() {
                    state.insert(key.to_owned(), n);
                }
                Message::Data(DataMessage::new(key, mapped, time))
            }
            Message::Interrogate(mut x) => {
                println!("Adding state to interrogate");
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
            Message::AbsBarrier(mut b) => {
                b.persist(&state, &ctx.operator_id);
                Message::AbsBarrier(b)},
            // Message::Load(l) => {
            //     state = l.load(ctx.operator_id).unwrap_or_default();
            //     Message::Load(l)
            // }
            Message::Rescale(x) => Message::Rescale(x),
            Message::ShutdownMarker(x) => Message::ShutdownMarker(x),
            Message::Epoch(x) => Message::Epoch(x),
        };

        state_size.set(state.len() as f64);
        output.send(mapped)
    }
}

impl<K, VI, T> StatefulMap<K, VI, T> for JetStreamBuilder<K, VI, T>
where
    K: Key + Serialize + DeserializeOwned,
    VI: DistData,
    T: MaybeTime,
{
    fn stateful_map<VO: Data, S: Default + Serialize + DeserializeOwned + 'static>(
        self,
        mapper: impl FnMut(&K, VI, S) -> (VO, Option<S>) + 'static,
    ) -> JetStreamBuilder<K, VO, T> {
        let op = OperatorBuilder::built_by(move |ctx| build_stateful_map(ctx, mapper));
        self.then(op)
    }
}
