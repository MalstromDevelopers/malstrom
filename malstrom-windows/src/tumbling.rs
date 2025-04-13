use std::{
    hash::Hash,
    ops::{Add, Rem, Sub},
};

use malstrom::{
    keyed::distributed::{DistData, DistKey},
    operators::RichFunction,
    runtime::communication::Distributable,
    stream::JetStreamBuilder,
    types::{DataMessage, Message, Timestamp},
};

pub trait TumblingWindow<K, V, Duration, VO> {
    /// Tumbling event time window which provides the owned values arrived on time
    /// during the windows duration to the aggregator function. The window is triggered as soon
    /// as the epoch advances past the windows end time.
    ///
    /// # Example
    /// ```rust
    /// use malstrom::operators::*;
    /// use malstrom::operators::Source;
    /// use malstrom::runtime::{WorkerBuilder, threaded::SingleThreadRuntimeFlavor};
    /// use malstrom::testing::VecSink;
    /// use malstrom::sources::SingleIteratorSource;
    /// use malstrom::runtime::StreamProvider;
    ///
    /// let sink = VecSink::new();
    ///
    /// let mut worker = WorkerBuilder::new(SingleThreadRuntimeFlavor::default());
    /// 
    /// let (on_time, _late) = worker
    ///     .new_stream()
    ///     .source("source", SingleIteratorSource::new(0..100))
    ///     .assign_timestamps("assigner", |msg| msg.timestamp)
    ///     .generate_epochs("generate", |_, t| t.to_owned());
    ///
    /// on_time
    ///     .key_local("key", |_| false)
    ///     .tumbling_window("count", 50, |x| x.into_iter().sum())
    ///     .sink("sink", sink.clone())
    ///     .finish();
    /// worker.build().expect("can build").execute();
    /// let out: Vec<i32> = sink.into_iter().map(|x| x.value).collect();
    /// assert_eq!(out, vec![0..49.iter().sum(), 50..100.iter().sum()]);
    /// ```
    fn tumbling_window(
        self,
        name: &str,
        size: Duration,
        aggregator: impl (FnMut(Vec<V>) -> VO) + 'static,
    ) -> JetStreamBuilder<K, VO, Duration>;
}

impl<K, V, Duration, VO> TumblingWindow<K, V, Duration, VO> for JetStreamBuilder<K, V, Duration>
where
    K: DistKey,
    V: DistData,
    VO: DistData,
    Duration: Timestamp
        + Distributable
        + Add<Output = Duration>
        + Sub<Output = Duration>
        + Rem<Output = Duration>
        + Hash
        + Eq
        + Copy,
{
    fn tumbling_window(
        self,
        name: &str,
        size: Duration,
        mut aggregator: impl (FnMut(Vec<V>) -> VO) + 'static,
    ) -> JetStreamBuilder<K, VO, Duration> {
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
    fn test_tumbling_window() {
        let collector = VecSink::new();

        let rt = get_test_rt(|provider| {
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
                .tumbling_window("join-two", 2, |x| x.join("|"))
                .sink("sink", StatelessSink::new(collector.clone()));
        });
        rt.execute().unwrap();
        assert_eq!(
            collector.into_iter().map(|x| x.value).collect::<Vec<_>>(),
            vec!["hello|world", "foo|bar", "oh|my"]
        );
    }
}
