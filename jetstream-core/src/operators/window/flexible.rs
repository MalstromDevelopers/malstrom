use std::{collections::BTreeMap, iter::once};

use crate::{
    keyed::distributed::{DistData, DistTimestamp},
    operators::stateful_transform::StatefulTransform,
    stream::jetstream::JetStreamBuilder,
    time::Timestamp,
    Data, DataMessage, Key, Message,
};
use itertools::Itertools;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::ops::Bound::Included;

struct FromEpoch<T>(T);
impl<K, V, T> TryFrom<&Message<K, V, T>> for FromEpoch<T>
where
    T: Timestamp,
{
    type Error = ();

    fn try_from(value: &Message<K, V, T>) -> Result<Self, Self::Error> {
        match value {
            Message::Epoch(e) => Ok(Self(e.clone())),
            _ => Err(()),
        }
    }
}

pub trait FlexibleWindower<K, V, T, A> {
    /// Called for a message with no open window
    /// to create the initial window state and closing time.
    fn initializer(&self, msg: &DataMessage<K, V, T>) -> Option<(T, A)>;

    /// Called to aggregate a new message into the window state
    fn aggregator(&self, msg: DataMessage<K, V, T>, window_state: &mut A, closing_time: &T);
}

pub trait FlexibleWindow<K, V, T, A> {
    /// The flexible window is an event-time based, non overlapping window which closes
    /// at a user defined time.
    /// The closing time may be chosen any time a message is encountered, for which
    /// no open window exists.
    ///
    /// The aggregated window state is emitted downstream upon closing.
    /// The emitted aggregations will bear the timestamp of the window close time
    fn flexible_window(
        self,
        // Can return None to discard the message and not create a new state
        initializer: impl Fn(&DataMessage<K, V, T>) -> Option<(A, T)> + 'static,
        aggregator: impl Fn(DataMessage<K, V, T>, &mut A, &T) + 'static,
    ) -> JetStreamBuilder<K, A, T>;
}

impl<K, V, T, A> FlexibleWindow<K, V, T, A> for JetStreamBuilder<K, V, T>
where
    K: Key + Serialize + DeserializeOwned,
    V: DistData,
    T: DistTimestamp + Timestamp,
    A: DistData + Default,
{
    fn flexible_window(
        self,
        initializer: impl Fn(&DataMessage<K, V, T>) -> Option<(A, T)> + 'static,
        aggregator: impl Fn(DataMessage<K, V, T>, &mut A, &T) + 'static,
    ) -> JetStreamBuilder<K, A, T> {
        self.stateful_transform(
            move |msg, mut key_state: BTreeMap<T, A>, _output| {
                // get all states where closing time < msg time
                // this should only ever be one or none
                let mut open_windows = key_state.range_mut(T::MIN..msg.timestamp.clone());
                let window_state = open_windows.next();
                // only one
                debug_assert!(open_windows.next().is_none());

                let (closing_time, window_state) = if let Some((k, v)) = window_state {
                    (k.clone(), v)
                } else {
                    if let Some((v, k)) = initializer(&msg) {
                        let should_not_exist = key_state.insert(k.clone(), v);
                        debug_assert!(should_not_exist.is_none());
                        // PANIC: Can unwrap here since we just inserted the key
                        let window_state = key_state.get_mut(&k).unwrap();
                        (k, window_state)
                    } else {
                        return None;
                    }
                };
                aggregator(msg, window_state, &closing_time);
                Some(key_state)
            },
            |epoch: FromEpoch<T>, global_state, output| {
                let ts = epoch.0;

                global_state.retain(|global_key, key_state| {
                    // split off returns everything right of the key (including the key)
                    // so we need to swap
                    let mut to_emit = key_state.split_off(&ts);
                    std::mem::swap(&mut to_emit, key_state);

                    for (time, window_state) in
                        to_emit.into_iter().chain(key_state.remove_entry(&ts))
                    {
                        // PANIC: Can unwrap since we got the key from the iterator over k, v
                        output.send(Message::Data(DataMessage::new(
                            global_key.clone(),
                            window_state,
                            time,
                        )));
                    }
                    // remove any keys where we have emitted all aggregations
                    // TODO: Test
                    !key_state.is_empty()
                });
            },
        )
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
