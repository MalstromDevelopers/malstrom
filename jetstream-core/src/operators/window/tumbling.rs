use std::{
    collections::{BTreeMap, HashMap},
    marker::PhantomData,
};

use crate::{
    channels::selective_broadcast::{Receiver, Sender},
    operators::stateful_transform::StatefulTransform,
    stream::{BuildContext, JetStreamBuilder, Logic, OperatorBuilder, OperatorContext},
    types::{Data, DataMessage, Key, Message, Timestamp},
};

use indexmap::IndexMap;
use itertools::Itertools;
use serde::{de::DeserializeOwned, Serialize};

pub trait TumblingWindow<K, V, T, S> {
    /// The flexible window is an event-time based, non overlapping window which closes
    /// at a user defined time.
    /// The closing time may be chosen any time a message is encountered, for which
    /// no open window exists.
    ///
    /// The aggregated window state is emitted downstream upon closing.
    /// The emitted aggregations will bear the timestamp of the window close time
    fn tumbling_window(
        self,
        window_size: T,
        // Can return None to discard the message and not create a new state
        initializer: impl Fn(&DataMessage<K, V, T>) -> Option<S> + 'static,
        aggregator: impl Fn(DataMessage<K, V, T>, &mut S) + 'static,
    ) -> JetStreamBuilder<K, S, T>;
}

impl<K, V, T, S> TumblingWindow<K, V, T, S> for JetStreamBuilder<K, V, T>
where
K: Key + Serialize + DeserializeOwned,
V: Data,
T: Timestamp + Serialize + DeserializeOwned + std::ops::Add<Output = T>,
S: Data + Serialize + DeserializeOwned,
{
    fn tumbling_window(
        self,
        window_size: T,
        initializer: impl Fn(&DataMessage<K, V, T>) -> Option<S> + 'static,
        aggregator: impl Fn(DataMessage<K, V, T>, &mut S) + 'static,
    ) -> JetStreamBuilder<K, S, T> {

        let op = OperatorBuilder::built_by(move |ctx| {
            let mut windower_impl = FlexibleWindowImpl::new(ctx, window_size, initializer, aggregator);
            move |input, output, ctx| {windower_impl.run(input, output, ctx)}
        });
        self.then(op)

        // let op = OperatorBuilder::built_by(|ctx| )

        // self.stateful_transform(
        //     move |msg, mut key_state: BTreeMap<T, A>, _output| {
        //         // get all states where closing time < msg time
        //         // this should only ever be one or none
        //         let mut open_windows = key_state.range_mut(T::MIN..msg.timestamp.clone());
        //         let window_state = open_windows.next();
        //         // only one
        //         // TODO
        //         let fixme = open_windows.map(|x| x.0).collect_vec();
        //         debug_assert!(fixme.len() == 0, "{fixme:?}");

        //         let (closing_time, window_state) = if let Some((k, v)) = window_state {
        //             (k.clone(), v)
        //         } else if let Some((v, k)) = initializer(&msg) {
        //             let should_not_exist = key_state.insert(k.clone(), v);
        //             debug_assert!(should_not_exist.is_none());
        //             // PANIC: Can unwrap here since we just inserted the key
        //             let window_state = key_state.get_mut(&k).unwrap();
        //             (k, window_state)
        //         } else {
        //             return None;
        //         };
        //         aggregator(msg, window_state, &closing_time);
        //         Some(key_state)
        //     },
        //     |epoch: FromEpoch<T>, global_state, output| {
        //         let ts = epoch.0;

        //         global_state.retain(|global_key, key_state| {
        //             // split off returns everything right of the key (including the key)
        //             // so we need to swap
        //             let mut to_emit = key_state.split_off(&ts);
        //             std::mem::swap(&mut to_emit, key_state);

        //             for (time, window_state) in
        //                 to_emit.into_iter().chain(key_state.remove_entry(&ts))
        //             {
        //                 // PANIC: Can unwrap since we got the key from the iterator over k, v
        //                 output.send(Message::Data(DataMessage::new(
        //                     global_key.clone(),
        //                     window_state,
        //                     time,
        //                 )));
        //             }
        //             // remove any keys where we have emitted all aggregations
        //             // TODO: Test
        //             !key_state.is_empty()
        //         });
        //     },
        // )
    }
}


// TODO a lot of cloning going on with T
struct FlexibleWindowImpl<K, V, T, S, Initializer, Aggregator> {
    keyed_state: IndexMap<K, BTreeMap<T, S>>,
    window_size: T,
    phantom: PhantomData<V>,
    initializer: Initializer,
    aggregator: Aggregator,
}

impl<K, V, T, S, Initializer, Aggregator> FlexibleWindowImpl<K, V, T, S, Initializer, Aggregator>
where
    K: Key + Serialize + DeserializeOwned,
    V: Data,
    T: Timestamp + Serialize + DeserializeOwned + std::ops::Add<Output = T>,
    S: Data + Serialize + DeserializeOwned,
    Initializer: Fn(&DataMessage<K, V, T>) -> Option<S> + 'static,
    Aggregator: Fn(DataMessage<K, V, T>, &mut S) + 'static,
{
    fn new(ctx: &BuildContext, 
        window_size: T,
        initializer: Initializer,
        aggregator: Aggregator) -> Self {
        let keyed_state = ctx.load_state().unwrap_or_default();
        Self {
            keyed_state,
            window_size,
            phantom: PhantomData,
            initializer,
            aggregator,
        }
    }

    fn run(&mut self, input: &mut Receiver<K, V, T>, output: &mut Sender<K, S, T>, ctx: &OperatorContext) {
        let msg = match input.recv() {
            Some(x) => x,
            None => return,
        };
        match msg {
            Message::Data(data_message) => self.handle_data_msg(data_message),
            Message::Epoch(epoch) => {
                self.handle_epoch(epoch.clone(), output);
                output.send(Message::Epoch(epoch));
            },
            Message::AbsBarrier(mut barrier) => {
                barrier.persist(&self.keyed_state, &ctx.operator_id);
                output.send(Message::AbsBarrier(barrier));
            },
            Message::Rescale(rescale_message) => output.send(Message::Rescale(rescale_message)),
            Message::ShutdownMarker(shutdown_marker) => output.send(Message::ShutdownMarker(shutdown_marker)),
            Message::Interrogate(mut interrogate) => {
                interrogate.add_keys(self.keyed_state.keys());
                output.send(Message::Interrogate(interrogate));
            },
            Message::Collect(mut collect) => {
                if let Some(s) = self.keyed_state.swap_remove(&collect.key) {
                    collect.add_state(ctx.operator_id, s);
                }
                output.send(Message::Collect(collect));
            },
            Message::Acquire(acquire) => {
                if let Some(s) = acquire.take_state(&ctx.operator_id) {
                    self.keyed_state.insert(s.0, s.1);
                }
                output.send(Message::Acquire(acquire));
            },
            Message::DropKey(_) => unimplemented!(),
        }
    }

    fn handle_data_msg(&mut self, msg: DataMessage<K, V, T>) {
        let state = self.keyed_state.entry(msg.key.clone()).or_default();
        // get any window this msg may belong to
        let window_end_time = msg.timestamp .clone() + self.window_size.clone();
        let mut windows = state.range_mut(msg.timestamp.clone()..=window_end_time.clone());
        let window = windows.next();
        // only one window, non-overlapping
        debug_assert!(windows.next().is_none());

        let mut window_state = match window {
            Some(x) => x.1,
            None => {
                let new_state = (self.initializer)(&msg);
                if let Some(initial_state) = new_state {
                    state.insert(window_end_time.clone(), initial_state);
                    state.get_mut(&window_end_time).unwrap()
                } else {
                    // discard message
                    return;
                }
            }
        };
        (self.aggregator)(msg, &mut window_state);
    }

    /// returns and removes all window state where 
    fn handle_epoch(&mut self, epoch: T, output: &mut Sender<K, S, T>) {
        // emit all windows where end_time <= epoch
        self.keyed_state.retain(|key, windows| {
            let to_emit_keys = windows.range(T::MIN..=epoch.clone()).map(|(k, _)| k).cloned().collect_vec();
            for window_end in to_emit_keys.into_iter() {
                let state = windows.remove(&window_end).expect("Key exists");
                output.send(
                    Message::Data(DataMessage::new(key.clone(), state, window_end.clone()))
                );
            };
            // remove any empty key states
            windows.len() != 0
        });
    }
}

fn build_flexible_window_op<K, V, T, S>(
    ctx: &BuildContext,
    initializer: impl Fn(&DataMessage<K, V, T>) -> Option<(S, T)> + 'static,
    aggregator: impl Fn(DataMessage<K, V, T>, &mut S, &T) + 'static,
) -> impl Logic<K, V, T, K, S, T>
where
    K: Key,
    V: Data,
    T: Timestamp,
    S: Serialize + DeserializeOwned,
{
    // key = window end time, val = states
    let keyed_state: BTreeMap<T, Vec<S>> = BTreeMap::new();

    |input, output, _ctx| {
        let msg = match input.recv() {
            Some(x) => x,
            None => return,
        };
        match msg {
            Message::Data(data_message) => todo!(),
            Message::Epoch(_) => todo!(),
            Message::AbsBarrier(barrier) => todo!(),
            Message::Rescale(rescale_message) => todo!(),
            Message::ShutdownMarker(shutdown_marker) => todo!(),
            Message::Interrogate(interrogate) => todo!(),
            Message::Collect(collect) => todo!(),
            Message::Acquire(acquire) => todo!(),
            Message::DropKey(_) => todo!(),
        }
    }
}

// #[cfg(test)]
// mod tests {
//     use itertools::Itertools;
//     use super::*;
//     use crate::{operators::{map::Map, source::Source}, stream::jetstream::JetStreamBuilder, test::{collect_stream_values, OperatorTester}, Message};

//     #[test]
//     fn test_emit_agg() {
//         // concat strings for up to two time units after the last message was received
//         // this is essentially a session window
//         let mut tester: OperatorTester<bool, String, i32, bool, String, i32, ()> =
//         OperatorTester::new_built_by(move |ctx| {
//             build_flexible_window(ctx, |msg, mut agg: String, close: i32| {
//                 agg.push_str(msg.value);
//                 (agg, msg.time + 2)
//             })
//         });

//         tester.send_from_local(crate::Message::Data(DataMessage::new(false, "foo".to_owned(), 4)));
//         tester.send_from_local(crate::Message::Data(DataMessage::new(false, "bar".to_owned(), 5)));
//         // our window closes at 5+2=7 (exclusive) so this should fall into a different window
//         tester.send_from_local(crate::Message::Data(DataMessage::new(false, "baz".to_owned(), 7)));
//         // this should go into the same one, despite being out of order
//         tester.send_from_local(crate::Message::Data(DataMessage::new(false, "boi".to_owned(), 6)));

//         // close the first one
//         tester.send_from_local(Message::Epoch(7));
//         // recv epoch
//         assert!(matches!(tester.receive_on_local().unwrap(), Message::Epoch(7)));
//         let agg1 = match tester.receive_on_local().unwrap() {
//             Message::Data(d) => d,
//             _ => panic!()
//         };
//         assert_eq!(agg1.key, false);
//         assert_eq!(agg1.value, "foobarbaz".to_owned());
//         // should be window close time
//         assert_eq!(agg1.timestamp, 7);

//         // lets close the next window
//         tester.send_from_local(Message::Epoch(9));
//         // recv epoch
//         assert!(matches!(tester.receive_on_local().unwrap(), Message::Epoch(9)));
//         let agg1 = match tester.receive_on_local().unwrap() {
//             Message::Data(d) => d,
//             _ => panic!()
//         };
//         assert_eq!(agg1.key, false);
//         assert_eq!(agg1.value, "boi".to_owned());
//         // should be window close time
//         assert_eq!(agg1.timestamp, 9);
//     }
// }
