use crate::{
    stream::StreamBuilder,
    types::{Data, MaybeKey, Timestamp},
};

/// Output messages from a Malstrom stream somewhere
pub trait Sink<K, V, T, S>: super::sealed::Sealed {
    /// Sink all messages in this stream to the given output.
    /// This will consume the messages. If you whish to write to multiple outputs,
    /// consider calling [.cloned()](crate::operators::Cloned::cloned) on the stream.
    ///
    /// # Example
    ///
    /// ```
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
    ///         .sink("sink", StatelessSink::new(sink_clone));
    ///     })
    ///     .execute()
    ///     .unwrap();
    /// let expected: Vec<i32> = (0..10).collect();
    /// let out: Vec<i32> = sink.into_iter().map(|x| x.value).collect();
    /// assert_eq!(out, expected);
    /// ```
    fn sink(self, name: &str, sink: S);
}

/// A stream output which takes messages, usually producing them to some external system.
/// For users it is normally not necessary to implement this trait unless they are writing
/// custom outputs for sinks which Malstrom does not (yet) support.
#[diagnostic::on_unimplemented(message = "Not a Sink: 
    You might need to wrap this in `StatefulSink::new` or `StatelessSink::new`")]
pub trait StreamSink<K, V, T> {
    /// Consume a datastream to the end.
    fn consume_stream(self, name: &str, builder: StreamBuilder<K, V, T>);
}

impl<K, V, T, S> Sink<K, V, T, S> for StreamBuilder<K, V, T>
where
    K: MaybeKey,
    V: Data,
    T: Timestamp,
    S: StreamSink<K, V, T>,
{
    fn sink(self, name: &str, sink: S) {
        sink.consume_stream(name, self)
    }
}
