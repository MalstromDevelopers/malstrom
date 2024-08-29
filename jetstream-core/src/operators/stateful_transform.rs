

use indexmap::IndexMap;
use itertools::Itertools;
use metrics::gauge;
use serde::{de::DeserializeOwned, Serialize};
use tracing::{event, Level};

use crate::{
    channels::selective_broadcast::{Receiver, Sender},
    keyed::distributed::DistData,
    stream::{
        jetstream::JetStreamBuilder,
        operator::{BuildContext, OperatorBuilder, OperatorContext},
    },
    time::Timestamp,
    Data, DataMessage, Key, Message,
};

/// This trait allows us to effectively 'subsribe' to message types
/// of messages
trait SubscribedMessage<K, V, T>: for<'a> TryFrom<&'a Message<K, V, T>, Error = ()> + 'static {}
impl<K, V, T, X> SubscribedMessage<K, V, T> for X where
    X: for<'a> TryFrom<&'a Message<K, V, T>, Error = ()> + 'static
{
}

pub trait StatefulTransform<K, VI, T> {
    /// Transforms message utilizing some managed state.
    ///
    /// This operator will apply a transforming function to every message.
    /// **NOTE:** Most user will likely prefer the ergonomics of [`super::stateful_map::StatefulMap::stateful_map`]
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
    /// ```
    fn stateful_transform<
        VO: Data,
        M: SubscribedMessage<K, VI, T>,
        S: Default + Serialize + DeserializeOwned + 'static,
    >(
        self,
        mapper: impl FnMut(DataMessage<K, VI, T>, S, &mut Sender<K, VO, T>) -> Option<S> + 'static,
        handler: impl FnMut(M, &mut IndexMap<K, S>, &mut Sender<K, VO, T>) -> () + 'static,
    ) -> JetStreamBuilder<K, VO, T>;
}

fn build_stateful_transform<
    K: Key + Serialize + DeserializeOwned,
    VI,
    T: Timestamp,
    VO: Clone,
    M: SubscribedMessage<K, VI, T>,
    S: Default + Serialize + DeserializeOwned,
>(
    context: &BuildContext,
    mut mapper: impl FnMut(DataMessage<K, VI, T>, S, &mut Sender<K, VO, T>) -> Option<S> + 'static,
    mut handler: impl FnMut(M, &mut IndexMap<K, S>, &mut Sender<K, VO, T>) -> () + 'static,
) -> impl FnMut(&mut Receiver<K, VI, T>, &mut Sender<K, VO, T>, &mut OperatorContext) {
    let mut state: IndexMap<K, S> = context.load_state().unwrap_or_default();
    let state_size = gauge!("{}.stateful_map.state_size", "label" => format!("{}", context.label));

    move |input: &mut Receiver<K, VI, T>, mut output: &mut Sender<K, VO, T>, ctx| {
        let msg = match input.recv() {
            Some(x) => x,
            None => return,
        };

        // invoke generic message handler
        match &msg {
            Message::Data(_) => None,
            x => M::try_from(x)
                .ok()
                .map(|x| handler(x, &mut state, &mut output)),
        };

        match msg {
            Message::Data(d) => {
                event!(
                    Level::DEBUG,
                    state_key_space = state.len(),
                    worker_id = ctx.worker_id
                );

                let key = d.key.clone();
                let st = state.swap_remove(&key).unwrap_or_default();

                let mut new_state = mapper(d, st, output);
                if let Some(n) = new_state.take() {
                    state.insert(key.to_owned(), n);
                }
            }
            Message::Interrogate(mut x) => {
                x.add_keys(&(state.keys().map(|k| k.to_owned()).collect_vec()));
                output.send(Message::Interrogate(x))
            }
            Message::Collect(mut c) => {
                if let Some(x) = state.get(&c.key) {
                    c.add_state(ctx.operator_id, x);
                }
                output.send(Message::Collect(c))
            }
            Message::Acquire(a) => {
                if let Some(st) = a.take_state(&ctx.operator_id) {
                    state.insert(st.0, st.1);
                }
                output.send(Message::Acquire(a))
            }
            Message::DropKey(k) => {
                state.swap_remove(&k);
                output.send(Message::DropKey(k))
            }
            // necessary to convince Rust it is a different generic type now
            Message::AbsBarrier(mut b) => {
                b.persist(&state, &ctx.operator_id);
                output.send(Message::AbsBarrier(b))
            }
            // Message::Load(l) => {
            //     state = l.load(ctx.operator_id).unwrap_or_default();
            //     Message::Load(l)
            // }
            Message::Rescale(x) => output.send(Message::Rescale(x)),
            Message::ShutdownMarker(x) => output.send(Message::ShutdownMarker(x)),
            Message::Epoch(x) => output.send(Message::Epoch(x)),
        };

        state_size.set(state.len() as f64);
    }
}

impl<K, VI, T> StatefulTransform<K, VI, T> for JetStreamBuilder<K, VI, T>
where
    K: Key + Serialize + DeserializeOwned,
    VI: DistData,

    T: Timestamp,
{
    fn stateful_transform<
        VO: Data,
        M: SubscribedMessage<K, VI, T>,
        S: Default + Serialize + DeserializeOwned + 'static,
    >(
        self,
        mapper: impl FnMut(DataMessage<K, VI, T>, S, &mut Sender<K, VO, T>) -> Option<S> + 'static,
        handler: impl FnMut(M, &mut IndexMap<K, S>, &mut Sender<K, VO, T>) -> () + 'static,
    ) -> JetStreamBuilder<K, VO, T> {
        let op =
            OperatorBuilder::built_by(move |ctx| build_stateful_transform(ctx, mapper, handler));
        self.then(op)
    }
}
