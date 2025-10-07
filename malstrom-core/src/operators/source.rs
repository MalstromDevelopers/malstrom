use crate::{
    stream::{InitialStreamBuilder, Malstrom, StreamBuilder},
    types::{Data, Kvt, MaybeKey, NoData, NoKey, NoTime, Sealed, Timestamp},
};

/// Produce new messages into a datastream.
pub trait Source<M: Kvt, S>: Sealed {
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
    fn source(self, name: &str, source: S) -> StreamBuilder<M>;
}

#[diagnostic::on_unimplemented(message = "Not a Source: 
    You might need to wrap this in `StatefulSource::new` or `StatelessSource::new`")]
/// A stream input which produces messages, usually reading them from some external system.
/// For users it is normally not necessary to implement this trait unless they are writing
/// custom inputs for sources which Malstrom does not (yet) support.
pub trait StreamSource<M: Kvt> {
    /// Turn this source into a stream by consuming the given stream builder.
    /// Source operators **must** read their inputs and forward all system messages downstream.
    fn into_stream(self, name: &str, builder: InitialStreamBuilder) -> StreamBuilder<M>;
}

impl<M, S> Source<M, S> for InitialStreamBuilder
where
    M: Kvt,
    S: StreamSource<M>,
{
    fn source(self, name: &str, source: S) -> StreamBuilder<M> {
        source.into_stream(name, self)
    }
}
