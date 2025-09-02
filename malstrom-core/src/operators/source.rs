use crate::{
    stream::StreamBuilder,
    types::{Data, MaybeKey, NoData, NoKey, NoTime, Timestamp},
};

/// Produce new messages into a datastream.
pub trait Source<K, V, T, S>: super::sealed::Sealed {
    /// Produce new messages into a stream. This method can only be called
    /// on a stream which does not yet have any other source. To use multiple sources
    /// create multiple streams and merge them by calling (.union())[StreamBuilder::union].
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
    fn source(self, name: &str, source: S) -> StreamBuilder<K, V, T>;
}

#[diagnostic::on_unimplemented(message = "Not a Source: 
    You might need to wrap this in `StatefulSource::new` or `StatelessSource::new`")]
/// A stream input which produces messages, usually reading them from some external system.
/// For users it is normally not necessary to implement this trait unless they are writing
/// custom inputs for sources which Malstrom does not (yet) support.
pub trait StreamSource<K, V, T> {
    /// Turn this source into a stream by consuming the given stream builder.
    /// Source operators **must** read their inputs and forward all system messages downstream.
    fn into_stream(
        self,
        name: &str,
        builder: StreamBuilder<NoKey, NoData, NoTime>,
    ) -> StreamBuilder<K, V, T>;
}

impl<K, V, T, S> Source<K, V, T, S> for StreamBuilder<NoKey, NoData, NoTime>
where
    K: MaybeKey,
    V: Data,
    T: Timestamp,
    S: StreamSource<K, V, T>,
{
    fn source(self, name: &str, source: S) -> StreamBuilder<K, V, T> {
        source.into_stream(name, self)
    }
}
