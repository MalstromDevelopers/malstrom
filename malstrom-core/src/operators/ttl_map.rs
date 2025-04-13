use std::hash::Hash;

use expiremap::ExpiredIterator;
pub use expiremap::{ExpireMap, ExpiryEntry};
use serde::{de::DeserializeOwned, Serialize};

use crate::{
    channels::operator_io::Output,
    stream::StreamBuilder,
    types::{Data, DataMessage, Key, MaybeData, Timestamp},
};

use super::stateful_op::{StatefulLogic, StatefulOp};

// TODO: Documentation ENUM to support variants

pub trait TtlMap<K, VI, T>: super::sealed::Sealed
where
    T: Serialize + DeserializeOwned,
{
    /// Transforms data utilizing some managed state. The state is subject to a TTL.
    /// On every epoch advancement the on_epoch function will expire and remove old keys.
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
    /// ```rust
    /// use malstrom::operators::*;
    /// use malstrom::runtime::SingleThreadRuntime;
    /// use malstrom::snapshot::NoPersistence;
    /// use malstrom::sources::{SingleIteratorSource, StatelessSource};
    /// use malstrom::worker::StreamProvider;
    /// use malstrom::sinks::{VecSink, StatelessSink};
    /// use malstrom::types::{DataMessage, Message};
    /// use malstrom::channels::operator_io::Output;
    ///
    /// let sink = VecSink::new();
    /// let sink_clone = sink.clone();
    ///
    /// SingleThreadRuntime::builder()
    ///     .persistence(NoPersistence)
    ///     .build(move |provider: &mut dyn StreamProvider| {
    ///         provider.new_stream()
    ///         .source("numbers", StatelessSource::new(SingleIteratorSource::new(0..50)))
    ///         // stateful operators require a keyed stream
    ///         .key_local("key", |_| false)
    ///         // sum up all numbers and thus far
    ///         .ttl_map(
    ///             "ttl", |key, inp, ts, mut state: ExpireMap<String, i32, usize>, out: &mut Output<bool, i32, usize>| {
    ///                 let g = state.get(&"key".to_owned());
    ///                 let val = if let Some(val) = g {
    ///                     let v = inp + *val;
    ///                     state.insert("key".to_owned(), v, ts + 15);
    ///                     v
    ///                 } else {
    ///                     state.insert("key".to_owned(), inp, ts + 15);
    ///                     inp
    ///                 };
    ///                 out.send(Message::Data(DataMessage::new(*key, val, ts)));
    ///                 Some(state)
    ///             },
    ///             |_, _, _, _| {}
    ///         )
    ///         .sink("sink", StatelessSink::new(sink_clone));
    ///     })
    ///     .execute()
    ///     .unwrap();
    ///
    /// let expected: Vec<i32> = (0..50).scan(0, |state, x| {*state = *state + x; Some(*state)}).collect();
    /// let out: Vec<i32> = sink.into_iter().map(|x| x.value).collect();
    /// assert_eq!(out, expected);
    /// ```
    fn ttl_map<
        VO: Data,
        S: Default + Serialize + DeserializeOwned + 'static,
        UK: Eq + Hash + Clone + Serialize + DeserializeOwned + 'static,
    >(
        self,
        name: &str,
        function: impl FnMut(
                &K,
                VI,
                T,
                ExpireMap<UK, S, T>,
                &mut Output<K, VO, T>,
            ) -> Option<ExpireMap<UK, S, T>>
            + 'static,
        expired_function: impl FnMut(&K, &T, ExpiredIterator<UK, ExpiryEntry<S, T>>, &mut Output<K, VO, T>)
            + 'static,
    ) -> StreamBuilder<K, VO, T>;
}

struct TtlOp<F, G> {
    function: F,
    expired_function: G,
}
impl<F, G> TtlOp<F, G> {
    fn new(function: F, expired_function: G) -> Self {
        Self {
            function,
            expired_function,
        }
    }
}

impl<F, G, K, VI, T, VO, UK, S> StatefulLogic<K, VI, T, VO, ExpireMap<UK, S, T>>
    for TtlOp<F, G>
where
    K: Key,
    UK: Key,
    VO: MaybeData,
    T: Timestamp,
    F: FnMut(&K, VI, T, ExpireMap<UK, S, T>, &mut Output<K, VO, T>) -> Option<ExpireMap<UK, S, T>>
        + 'static,
    G: FnMut(&K, &T, ExpiredIterator<UK, ExpiryEntry<S, T>>, &mut Output<K, VO, T>) + 'static,
    S: Serialize + DeserializeOwned,
{
    fn on_data(
        &mut self,
        msg: DataMessage<K, VI, T>,
        key_state: ExpireMap<UK, S, T>,
        output: &mut Output<K, VO, T>,
    ) -> Option<ExpireMap<UK, S, T>> {
        (self.function)(&msg.key, msg.value, msg.timestamp, key_state, output)
    }

    fn on_epoch(
        &mut self,
        epoch: &T,
        state: &mut indexmap::IndexMap<K, ExpireMap<UK, S, T>>,
        output: &mut Output<K, VO, T>,
    ) -> () {
        state.retain(|k, v| {
            let expired = v.expire(epoch);
            (self.expired_function)(k, epoch, expired, output);
            !v.is_empty()
        });
    }
}

impl<K, VI, T> TtlMap<K, VI, T> for StreamBuilder<K, VI, T>
where
    K: Key + Serialize + DeserializeOwned,
    VI: Data + Serialize + DeserializeOwned,
    T: Timestamp + Serialize + DeserializeOwned,
{
    fn ttl_map<
        VO: Data,
        S: Default + Serialize + DeserializeOwned + 'static,
        UK: Eq + Hash + Clone + Serialize + DeserializeOwned + 'static,
    >(
        self,
        name: &str,
        function: impl FnMut(
                &K,
                VI,
                T,
                ExpireMap<UK, S, T>,
                &mut Output<K, VO, T>,
            ) -> Option<ExpireMap<UK, S, T>>
            + 'static,
        expired_function: impl FnMut(&K, &T, ExpiredIterator<UK, ExpiryEntry<S, T>>, &mut Output<K, VO, T>)
            + 'static,
    ) -> StreamBuilder<K, VO, T> {
        self.stateful_op(name, TtlOp::new(function, expired_function))
    }
}

#[cfg(test)]
mod test {

    use expiremap::ExpireMap;
    use itertools::Itertools;

    use crate::channels::operator_io::Output;
    use crate::operators::source::Source;
    use crate::operators::{AssignTimestamps, GenerateEpochs, KeyLocal, Sink};

    use crate::sinks::StatelessSink;
    use crate::sources::{SingleIteratorSource, StatelessSource};
    use crate::testing::{get_test_rt, VecSink};
    use crate::types::{DataMessage, Message};

    use super::TtlMap;

    /// Simple test to check we are keeping state
    #[test]
    fn keeps_state() {
        let collector = VecSink::new();

        let rt = get_test_rt(|provider| {
            let (on_time, _late) = provider
                .new_stream()
                .source(
                    "source",
                    StatelessSource::new(SingleIteratorSource::new(0..100)),
                )
                .assign_timestamps("assigner", |msg| msg.timestamp)
                .generate_epochs("generate", |_, t| t.to_owned());

            // calculate a running total split by odd and even numbers
            on_time
                .key_local("key-local", |x| (x.value & 1) == 1)
                .ttl_map(
                    "add",
                    |key,
                     inp,
                     ts,
                     mut state: ExpireMap<String, i32, usize>,
                     out: &mut Output<bool, i32, usize>| {
                        let g = state.get(&"key".to_owned());
                        let val = if let Some(val) = g {
                            let v = inp + *val;
                            state.insert("key".to_owned(), v, ts + 15);
                            v
                        } else {
                            state.insert("key".to_owned(), inp, ts + 15);
                            inp
                        };

                        out.send(Message::Data(DataMessage::new(*key, val, ts)));

                        Some(state)
                    },
                    |_, _, _, _| {},
                )
                .sink("sink", StatelessSink::new(collector.clone()));
        });
        rt.execute().expect("Executing runtime failed");

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

    /// check we discard state on epoch advancement
    #[test]
    fn discards_state() {
        let collector = VecSink::new();
        let rt = get_test_rt(|provider| {
            let (on_time, _late) = provider
                .new_stream()
                .source(
                    "source",
                    StatelessSource::new(SingleIteratorSource::new(
                        ["foo", "bar", "hello", "world", "baz"].map(|x| x.to_string()),
                    )),
                )
                // concat the words
                .assign_timestamps("assigner", |msg| msg.timestamp)
                .generate_epochs("generator", |msg, _| Some(msg.timestamp));

            on_time
                .key_local("key-local", |_| 0)
                .ttl_map(
                    "concat",
                    |key,
                     inp,
                     ts,
                     mut state: ExpireMap<usize, String, usize>,
                     out: &mut Output<usize, String, usize>| {
                        state.insert(ts, inp, ts + 2);
                        let res = (0..=ts).filter_map(|i| state.get(&i)).join("|");

                        if !res.is_empty() {
                            out.send(Message::Data(DataMessage::new(*key, res, ts)));
                        }
                        Some(state)
                    },
                    |_, _, _, _| {},
                )
                .sink("sink", StatelessSink::new(collector.clone()));
        });

        rt.execute().expect("Executing runtime failed");

        let result = collector.into_iter().map(|x| x.value).collect_vec();
        let expected = vec![
            "foo",
            "foo|bar",
            "foo|bar|hello",
            "bar|hello|world",
            "hello|world|baz",
        ];
        assert_eq!(result, expected)
    }
}
