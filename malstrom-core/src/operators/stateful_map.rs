use serde::{de::DeserializeOwned, Serialize};

use crate::{
    channels::operator_io::Output,
    stream::StreamBuilder,
    types::{Data, DataMessage, Key, MaybeData, MaybeKey, MaybeTime, Message, Timestamp},
};

use super::stateful_op::{StatefulLogic, StatefulOp};

/// Apply a stateful function to every message in the stream
pub trait StatefulMap<K, VI, T>: super::sealed::Sealed {
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
    /// ```rust
    /// use malstrom::operators::*;
    /// use malstrom::runtime::SingleThreadRuntime;
    /// use malstrom::snapshot::NoPersistence;
    /// use malstrom::sources::{SingleIteratorSource, StatelessSource};
    /// use malstrom::worker::StreamProvider;
    /// use malstrom::sinks::{VecSink, StatelessSink};
    ///
    /// let sink = VecSink::new();
    /// let sink_clone = sink.clone();
    ///
    /// SingleThreadRuntime::builder()
    ///     .persistence(NoPersistence)
    ///     .build(move |provider: &mut dyn StreamProvider| {
    ///         provider.new_stream()
    ///         .source("numbers", StatelessSource::new(SingleIteratorSource::new(0..10)))
    ///         .key_local("key_local", |_| 0)
    ///         .stateful_map(
    ///             "statefule_map", |_key, value, state: i32| ((state + value), Some(state + value))
    ///         )
    ///         .sink("sink", StatelessSink::new(sink_clone));
    ///     })
    ///     .execute()
    ///     .unwrap();
    ///
    /// let expected: Vec<i32> = (0..10).scan(0, |state, x| {*state = *state + x; Some(*state)}).collect();
    /// let out: Vec<i32> = sink.into_iter().map(|x| x.value).collect();
    /// assert_eq!(out, expected);
    /// ```
    fn stateful_map<VO: Data, S: Default + Serialize + DeserializeOwned + 'static>(
        self,
        name: &str,
        mapper: impl FnMut(&K, VI, S) -> (VO, Option<S>) + 'static,
    ) -> StreamBuilder<K, VO, T>;
}

struct MapperOp<F> {
    mapper: F,
}
impl<F> MapperOp<F> {
    fn new(mapper: F) -> Self {
        Self { mapper }
    }
}

impl<F, K, VI, T, VO, S> StatefulLogic<K, VI, T, VO, S> for MapperOp<F>
where
    K: MaybeKey,
    VO: MaybeData,
    T: MaybeTime,
    F: FnMut(&K, VI, S) -> (VO, Option<S>) + 'static,
    S: Serialize + DeserializeOwned,
{
    fn on_data(
        &mut self,
        msg: DataMessage<K, VI, T>,
        key_state: S,
        output: &mut Output<K, VO, T>,
    ) -> Option<S> {
        let (new_value, new_state) = (self.mapper)(&msg.key, msg.value, key_state);
        let out_msg = DataMessage::new(msg.key, new_value, msg.timestamp);
        output.send(Message::Data(out_msg));
        new_state
    }
}

impl<K, VI, T> StatefulMap<K, VI, T> for StreamBuilder<K, VI, T>
where
    K: Key + Serialize + DeserializeOwned,
    VI: Data + Serialize + DeserializeOwned,
    T: Timestamp,
{
    fn stateful_map<VO: Data, S: Default + Serialize + DeserializeOwned + 'static>(
        self,
        name: &str,
        mapper: impl FnMut(&K, VI, S) -> (VO, Option<S>) + 'static,
    ) -> StreamBuilder<K, VO, T> {
        self.stateful_op(name, MapperOp::new(mapper))
    }
}

#[cfg(test)]
mod test {

    use itertools::Itertools;

    use crate::operators::source::Source;
    use crate::operators::{KeyLocal, Sink};

    use crate::sinks::StatelessSink;
    use crate::sources::{SingleIteratorSource, StatelessSource};
    use crate::testing::{get_test_rt, VecSink};

    use super::StatefulMap;

    /// Simple test to check we are keeping state
    #[test]
    fn keeps_state() {
        let collector = VecSink::new();

        let rt = get_test_rt(|provider| {
            provider
                .new_stream()
                .source(
                    "source",
                    StatelessSource::new(SingleIteratorSource::new(0..100)),
                )
                // calculate a running total split by odd and even numbers
                .key_local("key-local", |x| (x.value & 1) == 1)
                .stateful_map("add", |_, i, s: i32| (s + i, Some(s + i)))
                .sink("sink", StatelessSink::new(collector.clone()));
        });
        rt.execute().unwrap();

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
        let collector = VecSink::new();
        let rt = get_test_rt(|provider| {
            provider
                .new_stream()
                .source(
                    "source",
                    StatelessSource::new(SingleIteratorSource::new(
                        ["foo", "bar", "hello", "world", "baz"].map(|x| x.to_string()),
                    )),
                )
                // concat the words
                .key_local("key-local", |x| x.value.len())
                .stateful_map("concat", |_, x, mut s: String| {
                    s.push_str(&x);
                    if s.len() >= 6 {
                        (s, None)
                    } else {
                        (s.clone(), Some(s))
                    }
                })
                .sink("sink", StatelessSink::new(collector.clone()));
        });

        rt.execute().unwrap();

        let result = collector.into_iter().map(|x| x.value).collect_vec();
        let expected = vec!["foo", "foobar", "hello", "helloworld", "baz"];
        assert_eq!(result, expected)
    }
}
