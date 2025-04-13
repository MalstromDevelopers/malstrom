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
    /// use malstrom::StreamProvider;
    /// use malstrom::operators::*;
    /// use malstrom::sinks::{StatelessSink, StdOut};
    /// use malstrom::sources::{StatelessSource, SingleIteratorSource};
    ///
    /// fn build_stream(provider: &mut dyn StreamProvider) -> () {
    ///     provider
    ///         .new_stream()
    ///         .source(StatelessSource::new(SingleIteratorSource::new(0..10)))
    ///         .sink(StatelessSink::new(StdOut))
    /// }
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
