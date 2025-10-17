use std::hash::Hash;

pub use expiremap;
use expiremap::ExpireMap;
use serde::{Serialize, de::DeserializeOwned};

use crate::{
    channels::operator_io::Output,
    operators::State,
    stream::StreamBuilder,
    types::{Data, DataMessage, Key, Kvt, MaybeData, Message, Sealed, Timestamp},
};

use super::stateful_op::{StatefulLogic, StatefulOp};

/// Map with automatic state clean up based on ttl.
pub trait TtlMap<In: Kvt, OutVal: Data>: Sealed {
    /// Transforms data utilizing managed state where every value has a finite Time to Live (TTL).
    /// When an Epoch reaches this operator, all state values with a whos expiry time is less than
    /// or equal to the value of the Epoch will be removed from state.
    ///
    /// This operator applies a transforming function to every message.
    /// The function gets ownership of the state belonging to that message's
    /// key and can either return a new state or `None` to indicate, that
    /// the state for this key need not be retained.
    ///
    /// Any state can be used as long as it implements the `Default`, `Serialize`
    /// and `DeserializeOwned` traits.
    fn ttl_map<S, UK, F>(
        self,
        name: &str,
        function: F,
    ) -> StreamBuilder<(In::Key, OutVal, In::Timestamp)>
    where
        S: Default + Serialize + DeserializeOwned + 'static,
        UK: Eq + Hash + Clone + Serialize + DeserializeOwned + 'static,
        F: FnMut(
                &In::Key,
                In::Value,
                &In::Timestamp,
                ExpireMap<UK, S, In::Timestamp>,
            ) -> (OutVal, Option<ExpireMap<UK, S, In::Timestamp>>)
            + 'static;
}

struct TtlOp<F> {
    function: F,
}
impl<F> TtlOp<F> {
    fn new(function: F) -> Self {
        Self { function }
    }
}

impl<F, In, OutVal, S, UK> StatefulLogic<In, OutVal, ExpireMap<UK, S, In::Timestamp>> for TtlOp<F>
where
    In: Kvt,
    In::Key: State + Key,
    In::Timestamp: Ord,
    OutVal: Data,
    S: Serialize + DeserializeOwned,
    UK: Eq + Hash + Clone + Serialize + DeserializeOwned + 'static,
    F: FnMut(
            &In::Key,
            In::Value,
            &In::Timestamp,
            ExpireMap<UK, S, In::Timestamp>,
        ) -> (OutVal, Option<ExpireMap<UK, S, In::Timestamp>>)
        + 'static,
{
    fn on_data(
        &mut self,
        msg: DataMessage<In>,
        key_state: ExpireMap<UK, S, In::Timestamp>,
        output: &mut Output<(In::Key, OutVal, In::Timestamp)>,
    ) -> Option<ExpireMap<UK, S, In::Timestamp>> {
        let (value, state) = (self.function)(&msg.key, msg.value, &msg.timestamp, key_state);
        output.send(Message::Data(DataMessage::new(
            msg.key,
            value,
            msg.timestamp,
        )));
        state
    }

    fn on_epoch(
        &mut self,
        epoch: &In::Timestamp,
        state: &mut indexmap::IndexMap<In::Key, ExpireMap<UK, S, In::Timestamp>>,
        _output: &mut Output<(In::Key, OutVal, In::Timestamp)>,
    ) {
        state.retain(|_, v| {
            v.expire(epoch);
            !v.is_empty()
        });
    }
}

impl<In, OutVal> TtlMap<In, OutVal> for StreamBuilder<In>
where
    In: Kvt,
    In::Key: State + Key,
    In::Timestamp: State + Ord,
    OutVal: Data,
{
    fn ttl_map<S, UK, F>(
        self,
        name: &str,
        function: F,
    ) -> StreamBuilder<(In::Key, OutVal, In::Timestamp)>
    where
        S: Default + Serialize + DeserializeOwned + 'static,
        UK: Eq + Hash + Clone + Serialize + DeserializeOwned + 'static,
        F: FnMut(
                &In::Key,
                In::Value,
                &In::Timestamp,
                ExpireMap<UK, S, In::Timestamp>,
            ) -> (OutVal, Option<ExpireMap<UK, S, In::Timestamp>>)
            + 'static,
    {
        self.stateful_op(name, TtlOp::new(function))
    }
}

#[cfg(test)]
mod test {

    use expiremap::ExpireMap;
    use itertools::Itertools;

    use crate::operators::source::Source;
    use crate::operators::{AssignTimestamps, Filter, GenerateEpochs, KeyLocal, Sink};

    use crate::sinks::StatelessSink;
    use crate::sources::{SingleIteratorSource, StatelessSource};
    use crate::testing::{VecSink, get_test_rt};

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
                    |_key, inp, ts, mut state: ExpireMap<String, i32, usize>| {
                        let g = state.get(&"key".to_owned());
                        let val = if let Some(val) = g {
                            let v = inp + *val;
                            state.insert("key".to_owned(), v, ts + 15);
                            v
                        } else {
                            state.insert("key".to_owned(), inp, ts + 15);
                            inp
                        };
                        (val, Some(state))
                    },
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
                    |_key, inp, ts, mut state: ExpireMap<usize, String, usize>| {
                        state.insert(*ts, inp, ts + 2);
                        let res = (0..=*ts).filter_map(|i| state.get(&i)).join("|");
                        (res, Some(state))
                    },
                )
                .filter("remove-empty", |x| !x.is_empty())
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
