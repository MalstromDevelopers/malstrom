use crate::{
    stream::JetStreamBuilder,
    types::{Data, MaybeKey, Timestamp},
};

#[diagnostic::on_unimplemented(message = "Not a Sink: 
    You might need to wrap this in `StatefulSink::new` or `StatelessSink::new`")]
pub trait StreamSink<K, V, T> {
    fn consume_stream(self, name: &str, builder: JetStreamBuilder<K, V, T>) -> ();
}

pub trait Sink<K, V, T, S>: super::sealed::Sealed {
    fn sink(self, name: &str, sink: S) -> ();
}

impl<K, V, T, S> Sink<K, V, T, S> for JetStreamBuilder<K, V, T>
where
    K: MaybeKey,
    V: Data,
    T: Timestamp,
    S: StreamSink<K, V, T>,
{
    fn sink(self, name: &str, sink: S) -> () {
        sink.consume_stream(name, self)
    }
}
