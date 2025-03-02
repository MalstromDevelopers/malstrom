use std::{
    hash::Hash,
    ops::{Add, Rem, Sub},
};

use malstrom::{
    keyed::distributed::{DistData, DistKey, DistTimestamp},
    operators::RichFunction,
    runtime::communication::Distributable,
    stream::JetStreamBuilder,
    types::{DataMessage, Message, Timestamp},
};

pub trait TumblingWindow<K, V, T, VO> {
    // TODO: Update doc string
    /// Map transforms every value in a datastream into a different value
    /// by applying a given function or closure.
    ///
    /// # Example
    /// ```rust
    /// use malstrom::operators::*;
    /// use malstrom::operators::Source;
    /// use malstrom::runtime::{WorkerBuilder, threaded::SingleThreadRuntimeFlavor};
    /// use malstrom::testing::VecSink;
    /// use malstrom::sources::SingleIteratorSource;
    ///
    /// let sink = VecSink::new();
    ///
    /// let mut worker = WorkerBuilder::new(SingleThreadRuntimeFlavor::default());
    ///
    /// worker
    ///     .new_stream()
    ///     .source(SingleIteratorSource::new(0..100))
    ///     .map(|x| x * 2)
    ///     .sink(sink.clone())
    ///     .finish();
    ///
    /// worker.build().expect("can build").execute();
    /// let expected: Vec<i32> = (0..100).map(|x| x * 2).collect();
    /// let out: Vec<i32> = sink.into_iter().map(|x| x.value).collect();
    /// assert_eq!(out, expected);
    /// ```
    fn tumbling_window(
        self,
        name: &str,
        size: T,
        aggregator: impl (FnMut(Vec<V>) -> VO) + 'static,
    ) -> JetStreamBuilder<K, VO, T>;
}

impl<K, V, T, VO> TumblingWindow<K, V, T, VO> for JetStreamBuilder<K, V, T>
where
    K: DistKey,
    V: DistData,
    VO: DistData,
    T: Timestamp
        + Distributable
        + Add<Output = T>
        + Sub<Output = T>
        + Rem<Output = T>
        + Hash
        + Eq
        + Copy,
{
    fn tumbling_window(
        self,
        name: &str,
        size: T,
        mut aggregator: impl (FnMut(Vec<V>) -> VO) + 'static,
    ) -> JetStreamBuilder<K, VO, T> {
        self.rich_function(
            name,
            move |_, inp, ts, mut state, _| {
                // TODO: +size or +size-1?
                // TODO: What with the clone?
                let window_end = ts - (ts % size) + size;
                let res: Option<&mut Vec<V>> = state.get_mut(&window_end);

                if let Some(list) = res {
                    list.push(inp);
                } else {
                    state.insert(window_end, vec![inp], window_end);
                }

                Some(state)
            },
            move |k, _, window, out| {
                // This should at most contain one entry, as the window is tumbling.
                window.into_iter().for_each(|(_, e)| {
                    out.send(Message::Data(DataMessage::new(
                        k.clone(),
                        (aggregator)(e.value),
                        e.expiry,
                    )));
                });
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use malstrom::{
        keyed::KeyLocal,
        operators::{GenerateEpochs, Sink, Source, TimelyStream},
        sinks::StatelessSink,
        sources::{SingleIteratorSource, StatelessSource},
        testing::{VecSink, get_test_rt},
    };

    use crate::tumbling::TumblingWindow;

    #[test]
    fn test_map() {
        let collector = VecSink::new();

        let rt = get_test_rt(|provider| {
            let (on_time, _late) = provider
                .new_stream()
                .source(
                    "source",
                    StatelessSource::new(SingleIteratorSource::new([
                        // TODO: String slices do not work (&str) Then lifetime issues
                        "hello".to_owned(),
                        "world".to_owned(),
                        "foo".to_owned(),
                        "bar".to_owned(),
                        "oh".to_owned(),
                        "my".to_owned(),
                    ])),
                )
                .assign_timestamps("assigner", |msg| msg.timestamp)
                .generate_epochs("generate", |_, t| t.to_owned());

            on_time
                .key_local("key", |_| false)
                .tumbling_window("get-len", 2, |x| x.join("|"))
                .sink("sink", StatelessSink::new(collector.clone()));
        });
        rt.execute().unwrap();
        assert_eq!(
            collector.into_iter().map(|x| x.value).collect::<Vec<_>>(),
            vec!["hello|world", "foo|bar", "oh|my"]
        );
    }
}
