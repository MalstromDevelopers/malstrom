#[cfg(test)]
use std::thread::sleep;
#[cfg(test)]
use std::time::Duration;
use std::{
    hash::Hash,
    ops::{Add, Div, Rem, Sub},
};

use malstrom::{
    operators::{AssignTimestamps, GenerateEpochs},
    runtime::communication::Distributable,
    stream::StreamBuilder,
    types::{Key, MaybeData, Timestamp},
};
use num_traits::CheckedSub;

use crate::{get_processing_time, sliding::SlidingWindow};

pub trait SlidingProcessingTimeWindow<K, V, T, VO> {
    /// Sliding processing time window which passes a reference of all values part of the window
    /// on epoch update to the aggregator function and emit a record.
    ///
    /// If no records for a specific epoch are present, the aggregator funtion is not called.
    ///
    /// Careful: This resets the emitted epochs to the processing time in milliseconds since the UNIX epoch!
    ///
    /// Important: When testing is enabled to support tests an artifical sleep of 100ms is executed when assigning timestamps.
    ///
    /// # Example
    /// ```rust,no_run
    /// use malstrom::operators::*;
    /// use malstrom::runtime::SingleThreadRuntime;
    /// use malstrom::snapshot::NoPersistence;
    /// use malstrom::sources::{SingleIteratorSource, StatelessSource};
    /// use malstrom::worker::StreamProvider;
    /// use malstrom::sinks::{VecSink, StatelessSink};
    /// use malstrom_windows::processing::SlidingProcessingTimeWindow;
    ///
    /// let sink = VecSink::new();
    /// let sink_clone = sink.clone();
    ///
    /// SingleThreadRuntime::builder()
    ///     .persistence(NoPersistence)
    ///     .build(move |provider: &mut dyn StreamProvider| {
    ///         let (on_time, _late) = provider
    ///             .new_stream()
    ///             .source("source", StatelessSource::new(SingleIteratorSource::new(0..4)))
    ///             .assign_timestamps("assigner", |msg| msg.timestamp)
    ///             .generate_epochs("generate", |msg, _| Some(msg.timestamp));
    ///
    ///         on_time
    ///             .key_local("key", |_| false)
    ///             .sliding_processing_time_window("join-two", 199, |x| {
    ///                  x.iter().cloned().sum::<u64>()
    ///             })
    ///             .sink("sink", StatelessSink::new(sink_clone));
    ///      })
    ///      .execute()
    ///      .unwrap();
    /// let expected: Vec<_> = vec![0, 1, 1 + 2, 2 + 3];
    /// let out: Vec<_> = sink.into_iter().map(|x| x.value).collect();
    /// assert_eq!(out, expected);
    /// ```
    fn sliding_processing_time_window(
        self,
        name: &str,
        size: T,
        aggregator: impl (FnMut(&[&V]) -> VO) + 'static,
    ) -> StreamBuilder<K, VO, T>;
}

impl<K, V, T, VO> SlidingProcessingTimeWindow<K, V, u128, VO> for StreamBuilder<K, V, T>
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
    fn sliding_processing_time_window(
        self,
        name: &str,
        size: u128,
        aggregator: impl (FnMut(&[&V]) -> VO) + 'static,
    ) -> StreamBuilder<K, VO, u128> {
        let (stream, _late) = self
            .assign_timestamps(&format!("{name}-processing-timestamp"), |_| {
                let time = get_processing_time();

                #[cfg(test)]
                sleep(Duration::from_millis(100));

                time
            })
            .generate_epochs(&format!("{name}-processing-epoch"), |msg, _| {
                Some(msg.timestamp)
            });
        stream.sliding_window(
            &format!("{name}-processing-sliding-window"),
            size,
            aggregator,
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

    use crate::processing::SlidingProcessingTimeWindow;

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
                        StatelessSource::new(SingleIteratorSource::new(0..4)),
                    )
                    .assign_timestamps("assigner", |msg| msg.timestamp)
                    .generate_epochs("generate", |msg, _epoch| Some(msg.timestamp));

                on_time
                    .key_local("key", |_| false)
                    .sliding_processing_time_window("join-two", 199, |x| {
                        x.iter().cloned().sum::<u64>()
                    })
                    .sink("sink", StatelessSink::new(collector.clone()));
            });
        rt.execute().unwrap();
        assert_eq!(
            collector.into_iter().map(|x| x.value).collect::<Vec<_>>(),
            vec![0, 1, 1 + 2, 2 + 3]
        );
    }
}
