use std::{
    hash::Hash,
    ops::{Add, Rem, Sub},
};

use malstrom::{
    operators::TtlMap,
    runtime::communication::Distributable,
    stream::StreamBuilder,
    types::{DataMessage, Key, MaybeData, Message, Timestamp},
};

pub trait TumblingWindow<K, V, T, VO> {
    /// Tumbling event time window which provides the owned values arrived on time
    /// during the windows duration to the aggregator function. The window is triggered as soon
    /// as the epoch advances past the windows end time.
    ///
    /// # Example
    /// ```rust
    /// use malstrom::operators::*;
    /// use malstrom::runtime::SingleThreadRuntime;
    /// use malstrom::snapshot::NoPersistence;
    /// use malstrom::sources::{SingleIteratorSource, StatelessSource};
    /// use malstrom::worker::StreamProvider;
    /// use malstrom::sinks::{VecSink, StatelessSink};
    /// use malstrom_windows::tumbling::TumblingWindow;
    ///
    /// let sink = VecSink::new();
    /// let sink_clone = sink.clone();
    ///
    /// SingleThreadRuntime::builder()
    ///     .persistence(NoPersistence)
    ///     .build(move |provider: &mut dyn StreamProvider| {
    ///         let (on_time, _late) = provider
    ///             .new_stream()
    ///             .source("source", StatelessSource::new(SingleIteratorSource::new(0..100)))
    ///             .assign_timestamps("assigner", |msg| msg.timestamp)
    ///             .generate_epochs("generate", |_, t| t.to_owned());
    ///
    ///         on_time
    ///             .key_local("key", |_| false)
    ///             .tumbling_window("count", 50, 0, |x| x.into_iter().sum())
    ///             .sink("sink", StatelessSink::new(sink_clone));
    ///      })
    ///      .execute()
    ///      .unwrap();
    /// let expected: Vec<i32> = vec![(0..50).sum(), (50..100).sum()];
    /// let out: Vec<i32> = sink.into_iter().map(|x| x.value).collect();
    /// assert_eq!(out, expected);
    /// ```
    fn tumbling_window(
        self,
        name: &str,
        size: T,
        zero_alignemt: T,
        aggregator: impl (FnMut(Vec<V>) -> VO) + 'static,
    ) -> StreamBuilder<K, VO, T>;
}

impl<K, V, T, VO> TumblingWindow<K, V, T, VO> for StreamBuilder<K, V, T>
where
    K: Key + Distributable,
    V: MaybeData + Distributable,
    VO: MaybeData + Distributable,
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
        zero_alignemt: T,
        mut aggregator: impl (FnMut(Vec<V>) -> VO) + 'static,
    ) -> StreamBuilder<K, VO, T> {
        self.ttl_function(
            name,
            move |_, inp, ts, mut state, _| {
                // TODO: +size or +size-1?
                // TODO: What with the clone?
                let window_end = zero_alignemt + ts - ((ts + zero_alignemt) % size) + size;
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
        operators::{AssignTimestamps, GenerateEpochs, Sink, Source},
        runtime::SingleThreadRuntime,
        sinks::{StatelessSink, VecSink},
        snapshot::NoPersistence,
        sources::{SingleIteratorSource, StatelessSource},
        worker::StreamProvider,
    };

    use crate::tumbling::TumblingWindow;

    #[test]
    fn test_tumbling_window() {
        let collector = VecSink::new();

        let rt = SingleThreadRuntime::builder()
            .persistence(NoPersistence)
            .build(|provider: &mut dyn StreamProvider| {
                let (on_time, _late) = provider
                    .new_stream()
                    .source(
                        "source",
                        StatelessSource::new(SingleIteratorSource::new([
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
                    .tumbling_window("join-two", 2, 0, |x| x.join("|"))
                    .sink("sink", StatelessSink::new(collector.clone()));
            });
        rt.execute().unwrap();
        assert_eq!(
            collector.into_iter().map(|x| x.value).collect::<Vec<_>>(),
            vec!["hello|world", "foo|bar", "oh|my"]
        );
    }

    #[test]
    fn test_tumbling_window_alignment() {
        let collector = VecSink::new();

        let rt = SingleThreadRuntime::builder()
            .persistence(NoPersistence)
            .build(|provider: &mut dyn StreamProvider| {
                let (on_time, _late) = provider
                    .new_stream()
                    .source(
                        "source",
                        StatelessSource::new(SingleIteratorSource::new([
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
                    .tumbling_window("join-two", 2, 1, |x| x.join("|"))
                    .sink("sink", StatelessSink::new(collector.clone()));
            });
        rt.execute().unwrap();
        assert_eq!(
            collector.into_iter().map(|x| x.value).collect::<Vec<_>>(),
            vec!["hello", "world|foo", "bar|oh", "my"]
        );
    }
}
