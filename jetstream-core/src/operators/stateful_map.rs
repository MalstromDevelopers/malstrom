use std::collections::HashMap;

use itertools::Itertools;
use metrics::gauge;
use serde::{de::DeserializeOwned, Serialize};

use crate::{
    channels::selective_broadcast::{Receiver, Sender},
    stream::{BuildContext, JetStreamBuilder, OperatorBuilder, OperatorContext},
    types::{Data, DataMessage, Key, MaybeTime, Message, Timestamp},
};

pub trait StatefulMap<K, VI, T> {
    /// Transforms data utilizing some managed state.
    ///
    /// This operator will apply a transforming function to every message.
    /// The function gets ownership of the state belonging to that message's
    /// key and can either return a new state or `None` to indicate, that
    /// the state for this key need not be retained.
    ///
    /// Any state can be used as long as it implements the `Default`, `Serialize`
    /// and `DeserializeOwned` traits.
    /// A default is required, to create the inital state, when the state for a key
    /// does not yet exist (or has been dropped).
    /// The `Serialize` and `Deserialize` traits are required to make the state
    /// distributable on cluster resizes.
    ///
    /// # Examples
    ///
    /// This dataflow creates batches of 3 messages each
    ///
    /// ```
    /// # use jetstream::testing::{get_test_stream, VecSink};
    /// (builder, stream) = get_test_stream();
    ///
    /// let out = VecSink::new()
    /// stream
    ///     .source(SingleIteratorSource(0..7))
    ///     .stateful_map(
    ///         |value, mut state: Vec<i32>| {
    ///             if state.len() == 3 {
    ///                 (Some(state), None)
    ///             } else {
    ///                 state.push(value);
    ///                 (None, state)        
    ///             }
    ///         }
    ///     )
    ///    .sink(out.clone())
    ///    .finish()
    /// builder.build().unwrap().execute()
    /// assert_eq!(out.drain(..), vec![vec![0, 1, 2], vec![0, 1, 2]])
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
                let st = state.remove(&key).unwrap_or_default();
                let (mapped, mut new_state) = mapper(&key, value, st);
                if let Some(n) = new_state.take() {
                    state.insert(key.to_owned(), n);
                }
                Message::Data(DataMessage::new(key, mapped, time))
            }
            Message::Interrogate(mut x) => {
                x.add_keys(&(state.keys().map(|k| k.to_owned()).collect_vec()));
                Message::Interrogate(x)
            }
            Message::Collect(mut c) => {
                if let Some(x) = state.remove(&c.key) {
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
            // necessary to convince Rust it is a different generic type now
            Message::AbsBarrier(mut b) => {
                b.persist(&state, &ctx.operator_id);
                Message::AbsBarrier(b)
            }
            Message::Rescale(x) => Message::Rescale(x),
            Message::SuspendMarker(x) => Message::SuspendMarker(x),
            Message::Epoch(x) => Message::Epoch(x),
        };

        state_size.set(state.len() as f64);
        output.send(mapped)
    }
}

impl<K, VI, T> StatefulMap<K, VI, T> for JetStreamBuilder<K, VI, T>
where
    K: Key + Serialize + DeserializeOwned,
    VI: Data + Serialize + DeserializeOwned,
    T: Timestamp,
{
    fn stateful_map<VO: Data, S: Default + Serialize + DeserializeOwned + 'static>(
        self,
        mapper: impl FnMut(&K, VI, S) -> (VO, Option<S>) + 'static,
    ) -> JetStreamBuilder<K, VO, T> {
        let op = OperatorBuilder::built_by(move |ctx| build_stateful_map(ctx, mapper));
        self.then(op)
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;
    use std::rc::Rc;

    use indexmap::{IndexMap, IndexSet};
    use itertools::Itertools;

    use crate::keyed::distributed::{Acquire, Collect, Interrogate};

    use crate::operators::source::Source;
    use crate::operators::{KeyLocal, Sink};
    use crate::runtime::CommunicationClient;
    use crate::snapshot::{Barrier, PersistenceClient};
    use crate::sources::SingleIteratorSource;
    use crate::testing::{
        get_test_stream, CapturingPersistenceBackend, OperatorTester, VecSink
    };
    use crate::types::NoTime;
    use crate::types::{DataMessage, Message};

    use super::{build_stateful_map, StatefulMap};

    /// Simple test to check we are keeping state
    #[test]
    fn keeps_state() {
        let (builder, stream) = get_test_stream();

        let collector = VecSink::new();

        stream
            .source(SingleIteratorSource::new(0..100))
            // calculate a running total split by odd and even numbers
            .key_local(|x| (x.value & 1) == 1)
            .stateful_map(|_, i, s: i32| (s + i, Some(s + i)))
            .sink(collector.clone())
            .finish();

        builder.build().unwrap().execute();

        let result = collector.into_iter().map(|x| x.value).collect_vec();
        let even_sums = (0..100).step_by(2).scan(0, |s, i| {
            *s += i;
            Some(*s)
        });
        let odd_sums = (1..100).step_by(2).scan(0, |s, i| {
            *s += i;
            Some(*s)
        });
        let expected: Vec<i32> = even_sums.zip(odd_sums).flat_map(|x| [x.0, x.1]).collect();
        assert_eq!(result, expected)
    }

    /// check we discard state when requested
    #[test]
    fn discards_state() {
        let (builder, stream) = get_test_stream();
        let collector = VecSink::new();

        stream
            .source(SingleIteratorSource::new(
                ["foo", "bar", "hello", "world", "baz"].map(|x| x.to_string()),
            ))
            // concat the words
            .key_local(|x| x.value.len())
            .stateful_map(|_, x, mut s: String| {
                s.push_str(&x);
                if s.len() >= 6 {
                    (s, None)
                } else {
                    (s.clone(), Some(s))
                }
            })
            .sink(collector.clone())
            .finish();
        builder.build().unwrap().execute();

        let result = collector.into_iter().map(|x| x.value).collect_vec();
        let expected = vec!["foo", "foobar", "hello", "helloworld", "baz"];
        assert_eq!(result, expected)
    }

    #[test]
    fn test_interrogate() {
        let mut tester: OperatorTester<i32, String, NoTime, i32, (), NoTime, ()> = OperatorTester::built_by(move |ctx| build_stateful_map(ctx, |_, x, _| ((), Some(x))), 0, 0, 0..1);

        tester.send_local(Message::Data(DataMessage::new(1, "foo".to_string(), NoTime)));
        tester.step();
        tester.send_local(Message::Data(DataMessage::new(
            5,
            "bar".to_string(),
            NoTime,
        )));
        tester.step();

        let interrogator = Interrogate::new(Rc::new(|_: &i32| true));
        tester.send_local(Message::Interrogate(interrogator.clone()));
        tester.step();

        // receive and drop all messages. We need to drop the interrogator copy
        // so we can unwrap it
        while tester.recv_local().is_some() {}

        let result = interrogator.try_unwrap().unwrap();
        assert_eq!(IndexSet::from([1, 5]), result)
    }

    /// Check we do not add discarded keys
    #[test]
    fn test_interrogate_discarded() {
        let mut tester: OperatorTester<i32, String, NoTime, i32, (), NoTime, ()> =
            OperatorTester::built_by(move |ctx| {
                build_stateful_map(ctx, |_, x: String, _| {
                    ((), if x.len() > 3 { None } else { Some(x) })
                })
            }, 0, 0, 0..1);

        tester.send_local(Message::Data(DataMessage::new(
            1,
            "foo".to_string(),
            NoTime,
        )));
        tester.step();
        tester.send_local(Message::Data(DataMessage::new(
            1,
            "hello".to_string(),
            NoTime,
        )));
        tester.step();
        let interrogator = Interrogate::new(Rc::new(|_: &i32| true));
        tester.send_local(Message::Interrogate(interrogator.clone()));
        tester.step();

        // receive and drop all messages. We need to drop the interrogator copy
        // so we can unwrap it
        while tester.recv_local().is_some() {}

        let result = interrogator.try_unwrap().unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_collect() {
        let mut tester: OperatorTester<i32, String, NoTime, i32, (), NoTime, ()> =
            OperatorTester::built_by(move |ctx| {
                build_stateful_map(ctx, |_, x, _| ((), Some(x)))
            }, 0, 42, 0..1);

        tester.send_local(Message::Data(DataMessage::new(
            1,
            "foo".to_string(),
            NoTime,
        )));
        tester.step();
        tester.send_local(Message::Data(DataMessage::new(
            5,
            "bar".to_string(),
            NoTime,
        )));
        tester.step();
        let collector = Collect::new(1);
        tester.send_local(Message::Collect(collector.clone()));
        tester.step();

        // receive and drop all messages. We need to drop the interrogator copy
        // so we can unwrap it
        while tester.recv_local().is_some() {}

        let foo_enc = bincode::serde::encode_to_vec("foo", bincode::config::standard()).unwrap();
        let (_key, result) = collector.try_unwrap().unwrap();
        // 42 is the operator id
        assert_eq!(IndexMap::from([(42, foo_enc)]), result)
    }

    /// check we do not collect discarded state
    #[test]
    fn test_collect_discarded() {
        let mut tester: OperatorTester<i32, String, NoTime, i32, (), NoTime, ()> =
            OperatorTester::built_by(move |ctx| {
                build_stateful_map(ctx, |_, x: String, _| {
                    ((), if x.len() > 3 { None } else { Some(x) })
                })
            }, 0, 42, 0..1);

        tester.send_local(Message::Data(DataMessage::new(
            1,
            "foo".to_string(),
            NoTime,
        )));
        tester.step();
        tester.send_local(Message::Data(DataMessage::new(
            1,
            "hello".to_string(),
            NoTime,
        )));
        tester.step();
        let collector = Collect::new(1);
        tester.send_local(Message::Collect(collector.clone()));
        tester.step();

        // receive and drop all messages. We need to drop the interrogator copy
        // so we can unwrap it
        while tester.recv_local().is_some() {}

        let (_key, result) = collector.try_unwrap().unwrap();
        assert!(result.is_empty());
    }

    // check we acquire state when instructed
    #[test]
    fn test_acquire_state() {
        // just return the state for the key
        let mut tester: OperatorTester<i32, &str, NoTime, i32, String, NoTime, ()> =
            OperatorTester::built_by(move |ctx| {
                build_stateful_map(ctx, |_k, _v, s: String| (s.clone(), Some(s)))
            }, 0, 42, 0..1);

        let state = IndexMap::from([(42, CommunicationClient::encode("HelloWorld".to_owned()))]);

        tester.send_local(Message::Acquire(Acquire::new(
            1337,
            state,
        )));
        tester.step();
        tester.send_local(Message::Data(DataMessage::new(1337, "", NoTime)));
        tester.step();
        assert!(matches!(
            tester.recv_local().unwrap(),
            Message::Acquire(_)
        ));
        match tester.recv_local().unwrap() {
            Message::Data(DataMessage {
                key: 1337,
                value: x,
                timestamp: NoTime,
            }) => assert_eq!(x, "HelloWorld"),
            _ => panic!(),
        }
    }

    // check we drop key state when instructed
    #[test]
    fn test_drop_key_state() {
        // keep a total per key
        let mut tester: OperatorTester<bool, i32, NoTime, bool, i32, NoTime, ()> =
            OperatorTester::built_by(move |ctx| {
                build_stateful_map(ctx, |_k, v, s: i32| ((s + v), Some(s + v)))
            }, 0, 42, 0..1);

        tester.send_local(Message::Data(DataMessage::new(false, 1, NoTime)));
        tester.step();
        tester.recv_local().unwrap();
        tester.send_local(Message::Data(DataMessage::new(false, 2, NoTime)));
        tester.step();
        match tester.recv_local().unwrap() {
            Message::Data(d) => assert_eq!(d.value, 3),
            _ => panic!(),
        };

        tester.send_local(Message::Collect(Collect::new(false)));
        tester.step();
        tester.recv_local().unwrap();

        tester.send_local(Message::Data(DataMessage::new(false, 1, NoTime)));
        tester.step();
        // sum should be back to 1 since we dropped the state
        match tester.recv_local().unwrap() {
            Message::Data(d) => assert_eq!(d.value, 1),
            _ => panic!(),
        };
    }

    // check we snapshot state
    #[test]
    fn test_snapshot_state() {
        // keep a total per key
        let mut tester: OperatorTester<bool, i32, NoTime, bool, i32, NoTime, ()> =
            OperatorTester::built_by(move |ctx| {
                build_stateful_map(ctx, |_k, v, s: i32| ((s + v), Some(s + v)))
            },0, 42, 0..1);

        tester.send_local(Message::Data(DataMessage::new(false, 1, NoTime)));
        tester.step();

        let backend = CapturingPersistenceBackend::default();
        tester.send_local(Message::AbsBarrier(Barrier::new(Box::new(backend.clone()))));
        tester.step();

        let state: HashMap<bool, i32> = CommunicationClient::decode(&backend.load(&42).unwrap());
        assert_eq!(*state.get(&false).unwrap(), 1);
    }

    #[test]
    fn test_forward_system_messages() {
        let mut tester: OperatorTester<i32, String, NoTime, i32, (), NoTime, ()> =
            OperatorTester::built_by(move |ctx| {
                build_stateful_map(ctx, |_, x: String, _| {
                    ((), if x.len() > 3 { None } else { Some(x) })
                })
            },0 ,0 ,0..1);

        crate::testing::test_forward_system_messages(&mut tester);
    }
}
