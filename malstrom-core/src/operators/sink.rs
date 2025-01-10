use crate::{
    channels::operator_io::{Input, Output},
    stream::{JetStreamBuilder, OperatorBuilder},
    types::{Data, MaybeKey, Message, NoData, NoKey, NoTime, Timestamp},
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

pub(crate) trait SinkFull<K, V, T, S> {
    fn sink_full(self, name: &str, sink: S) -> ();
}
pub(crate) trait SinkFullImpl<K, V, T>: 'static {
    fn sink_full(&mut self, msg: Message<K, V, T>);
}

impl<K, V, T, S> SinkFull<K, V, T, S> for JetStreamBuilder<K, V, T>
where
    K: MaybeKey,
    V: Data,
    T: Timestamp,
    S: SinkFullImpl<K, V, T>,
{
    fn sink_full(self, name: &str, mut sink: S) -> () {
        self.then(OperatorBuilder::direct(
            name,
            move |input: &mut Input<K, V, T>, _output: &mut Output<NoKey, NoData, NoTime>, _ctx| {
                if let Some(msg) = input.recv() {
                    sink.sink_full(msg);
                }
            },
        ))
        .finish();
    }
}
