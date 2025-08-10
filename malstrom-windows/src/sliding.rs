use std::{
    hash::Hash,
    ops::{Add, Div, Rem, Sub},
};

use malstrom::{
    channels::operator_io::Output,
    operators::{StatefulLogic, StatefulOp, expiremap::ExpireMap, indexmap::IndexMap},
    runtime::communication::Distributable,
    stream::StreamBuilder,
    types::{DataMessage, Key, MaybeData, Message, Timestamp},
};
use num_traits::CheckedSub;
use serde::{Serialize, de::DeserializeOwned};

struct SlidingWindowOp<F, S> {
    aggregator: F,
    size: S,
}
impl<F, S> SlidingWindowOp<F, S> {
    fn new(function: F, size: S) -> Self {
        Self {
            aggregator: function,
            size,
        }
    }
}

impl<F, K, VI, T, VO> StatefulLogic<K, VI, T, VO, ExpireMap<T, VI, T>> for SlidingWindowOp<F, T>
where
    K: Key,
    VO: MaybeData,
    VI: Serialize + DeserializeOwned,
    T: Timestamp
        + Add<Output = T>
        + Sub<Output = T>
        + CheckedSub<Output = T>
        + Div<Output = T>
        + Eq
        + Hash
        + Clone
        + Serialize
        + DeserializeOwned
        + 'static,
    F: FnMut(&[&VI]) -> VO + 'static,
{
    fn on_data(
        &mut self,
        msg: DataMessage<K, VI, T>,
        mut key_state: ExpireMap<T, VI, T>,
        _output: &mut Output<K, VO, T>,
    ) -> Option<ExpireMap<T, VI, T>> {
        key_state.insert(
            msg.timestamp.clone(),
            msg.value,
            // Keep for double the size/duration to ensure its available on epoch
            msg.timestamp.clone() + self.size.clone() + self.size.clone(),
        );

        Some(key_state)
    }

    fn on_epoch(
        &mut self,
        epoch: &T,
        global_state: &mut IndexMap<K, ExpireMap<T, VI, T>>,
        output: &mut Output<K, VO, T>,
    ) {
        global_state.retain(|k, state| {
            // Need to prevent a negative wrap therefore do a checked sub
            let lower = epoch
                .clone()
                // subtract size - 1
                .checked_sub(&(self.size.clone() - (self.size.clone() / self.size.clone())))
                .unwrap_or(T::MIN);
            let window = state.get_range(|k, _| *k < lower, |k, _| *k <= epoch.clone());

            match window {
                // Only call the aggregator on non empty windows
                Some(window) if !window.is_empty() => {
                    let values = window.values().map(|o| &o.value).collect::<Vec<_>>();

                    output.send(Message::Data(DataMessage::new(
                        k.clone(),
                        (self.aggregator)(&values),
                        epoch.clone(),
                    )));
                }
                _ => (),
            }

            // Clean state after execution to ensure all entries are available.
            // Because of the range query too old entries will not be considered in any case.
            state.expire(epoch);
            !state.is_empty()
        });
    }
}

pub trait SlidingWindow<K, V, T, VO> {
    /// Sliding event time window which passes a reference of all values part of the window
    /// on epoch update to the aggregator function and emit a record.
    ///
    /// If no records for a specific epoch are present, the aggregator funtion is not called.
    ///
    /// Note: The step size is determined by the frequency of emitted epochs.
    ///
    /// # Example
    /// ```rust
    /// use malstrom::operators::*;
    /// use malstrom::runtime::SingleThreadRuntime;
    /// use malstrom::snapshot::NoPersistence;
    /// use malstrom::sources::{SingleIteratorSource, StatelessSource};
    /// use malstrom::worker::StreamProvider;
    /// use malstrom::sinks::{VecSink, StatelessSink};
    /// use malstrom_windows::sliding::SlidingWindow;
    ///
    /// let sink = VecSink::new();
    /// let sink_clone = sink.clone();
    ///
    /// SingleThreadRuntime::builder()
    ///     .persistence(NoPersistence)
    ///     .build(move |provider: &mut dyn StreamProvider| {
    ///         let (on_time, _late) = provider
    ///             .new_stream()
    ///             .source("source", StatelessSource::new(SingleIteratorSource::new(0..3)))
    ///             .assign_timestamps("assigner", |msg| msg.timestamp)
    ///             .generate_epochs("generate", |msg, _| Some(msg.timestamp));
    ///
    ///         on_time
    ///             .key_local("key", |_| false)
    ///             .sliding_window("count", 2, |x| x.iter().copied().sum())
    ///             .sink("sink", StatelessSink::new(sink_clone));
    ///      })
    ///      .execute()
    ///      .unwrap();
    /// let expected: Vec<i32> = vec![(0..1).sum(), (0..2).sum(), (1..3).sum()];
    /// let out: Vec<i32> = sink.into_iter().map(|x| x.value).collect();
    /// assert_eq!(out, expected);
    /// ```
    fn sliding_window(
        self,
        name: &str,
        size: T,
        aggregator: impl (FnMut(&[&V]) -> VO) + 'static,
    ) -> StreamBuilder<K, VO, T>;
}

impl<K, V, T, VO> SlidingWindow<K, V, T, VO> for StreamBuilder<K, V, T>
where
    K: Key + Distributable,
    V: MaybeData + Distributable,
    VO: MaybeData + Distributable,
    T: Timestamp
        + Distributable
        + Add<Output = T>
        + Sub<Output = T>
        + CheckedSub<Output = T>
        + Div<Output = T>
        + Rem<Output = T>
        + Hash
        + Eq
        + Copy,
{
    fn sliding_window(
        self,
        name: &str,
        size: T,
        aggregator: impl (FnMut(&[&V]) -> VO) + 'static,
    ) -> StreamBuilder<K, VO, T> {
        self.stateful_op(name, SlidingWindowOp::new(aggregator, size))
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

    use crate::sliding::SlidingWindow;

    #[test]
    fn test_sliding_window() {
        let collector = VecSink::new();

        let rt = SingleThreadRuntime::builder()
            .persistence(NoPersistence)
            .build(|provider: &mut dyn StreamProvider| {
                let (on_time, _late) = provider
                    .new_stream()
                    .source(
                        "source",
                        StatelessSource::new(SingleIteratorSource::new([
                            "one".to_owned(),
                            "two".to_owned(),
                            "three".to_owned(),
                            "four".to_owned(),
                        ])),
                    )
                    .assign_timestamps("assigner", |msg| msg.timestamp)
                    .generate_epochs("generate", |msg, _epoch| Some(msg.timestamp));

                on_time
                    .key_local("key", |_| false)
                    .sliding_window("join-two", 2, |x| {
                        x.iter()
                            .cloned()
                            .cloned()
                            .collect::<Vec<String>>()
                            .join("|")
                    })
                    .sink("sink", StatelessSink::new(collector.clone()));
            });
        rt.execute().unwrap();
        assert_eq!(
            collector.into_iter().map(|x| x.value).collect::<Vec<_>>(),
            vec!["one", "one|two", "two|three", "three|four"]
        );
    }
}
