
use serde::{de::DeserializeOwned, Serialize};

use crate::{
    channels::selective_broadcast::Sender,
    stream::JetStreamBuilder,
    types::{Data, DataMessage, Key, MaybeData, MaybeKey, MaybeTime, Message, Timestamp},
};

use super::stateful_op::{StatefulLogic, StatefulOp};

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
    /// ```rust
    /// use jetstream::operators::*;
    /// use jetstream::operators::Source;
    /// use jetstream::runtime::{WorkerBuilder, threaded::SingleThreadRuntimeFlavor};
    /// use jetstream::testing::VecSink;
    /// use jetstream::sources::SingleIteratorSource;
    ///
    /// let sink = VecSink::new();
    ///
    /// let mut worker = WorkerBuilder::new(SingleThreadRuntimeFlavor::default());
    ///
    /// worker
    ///     .new_stream()
    ///     .source(SingleIteratorSource::new(0..10))
    ///     // stateful operators require a keyed stream
    ///     .key_local(|_| 0)
    ///     // sum up all numbers and thus far
    ///     .stateful_map(
    ///         |_key, value, state: i32| ((state + value), Some(state + value)))
    ///     .sink(sink.clone())
    ///     .finish();
    ///
    /// worker.build().expect("can build").execute();
    /// let expected: Vec<i32> = (0..10).scan(0, |state, x| {*state = *state + x; Some(*state)}).collect();
    /// let out: Vec<i32> = sink.into_iter().map(|x| x.value).collect();
    /// assert_eq!(out, expected);
    /// ```
    fn stateful_map<VO: Data, S: Default + Serialize + DeserializeOwned + 'static>(
        self,
        mapper: impl FnMut(&K, VI, S) -> (VO, Option<S>) + 'static,
    ) -> JetStreamBuilder<K, VO, T>;
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
        output: &mut Sender<K, VO, T>,
    ) -> Option<S> {
        let (new_value, new_state) = (self.mapper)(&msg.key, msg.value, key_state);
        let out_msg = DataMessage::new(msg.key, new_value, msg.timestamp);
        output.send(Message::Data(out_msg));
        new_state
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
        self.stateful_op(MapperOp::new(mapper))
    }
}

#[cfg(test)]
mod test {
    
    

    
    use itertools::Itertools;

    

    use crate::operators::source::Source;
    use crate::operators::{KeyLocal, Sink};
    
    
    use crate::sources::SingleIteratorSource;
    use crate::testing::{get_test_stream, VecSink};
    
    

    use super::StatefulMap;

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
}
