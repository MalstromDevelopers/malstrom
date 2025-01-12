use std::{
    iter::{self, Enumerate, Peekable},
    marker::PhantomData,
};

use serde::{Deserialize, Serialize};

use crate::{
    channels::operator_io::{Input, Output},
    operators::StreamSource,
    stream::{JetStreamBuilder, OperatorBuilder},
    types::{Data, DataMessage, Message, NoData, NoKey, NoTime},
};

use super::{StatelessSourceImpl, StatelessSourcePartition};

/// A datasource which yields values from an iterator
/// on **one single worker**.
/// In the current implementation, values are always emitted
/// on worker `0`.
///
/// Emitted values are timestamped with their index in the iterator.
/// This sink emits an epoch `usize::MAX` after the last
/// element in the iterator.
///
/// # Example
/// ```rust
/// use malstrom::operators::*;
/// use malstrom::operators::Source;
/// use malstrom::runtime::{WorkerBuilder, threaded::SingleThreadRuntimeFlavor};
/// use malstrom::testing::VecSink;
/// use malstrom::sources::SingleIteratorSource;
///
/// let sink = VecSink::new();
/// let mut worker = WorkerBuilder::new(SingleThreadRuntimeFlavor::default());
///
/// worker
///     .new_stream()
///     .source(SingleIteratorSource::new(0..100))
///     .sink(sink.clone())
///     .finish();
///
/// worker.build().expect("can build").execute();
/// let expected: Vec<i32> = (0..100).collect();
/// let out: Vec<i32> = sink.into_iter().map(|x| x.value).collect();
/// assert_eq!(out, expected);
/// ```
pub struct SingleIteratorSource<T>(Option<Box<dyn Iterator<Item = T>>>);

impl<T> SingleIteratorSource<T> {
    pub fn new<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = T>,
        <I as IntoIterator>::IntoIter: 'static,
    {
        Self(Some(Box::new(iter.into_iter())))
    }
}

impl<V> StatelessSourceImpl<V, usize> for SingleIteratorSource<V>
where
    V: Data,
{
    type Part = NoKey;
    type SourcePartition = SingleIteratorPartition<V>;
    fn list_parts(&self) -> Vec<Self::Part> {
        vec![NoKey]
    }

    fn build_part(&mut self, _part: &Self::Part) -> Self::SourcePartition {
        match self.0.take() {
            Some(x) => SingleIteratorPartition(x.enumerate().peekable()),
            None => unreachable!("SingleIteratorSource only has one part"),
        }
    }
}

pub struct SingleIteratorPartition<V>(Peekable<Enumerate<Box<dyn Iterator<Item = V>>>>);

impl<V> StatelessSourcePartition<V, usize> for SingleIteratorPartition<V> {
    fn poll(&mut self) -> Option<(V, usize)> {
        self.0.next().map(|x| (x.1, x.0))
    }

    fn is_finished(&mut self) -> bool {
        self.0.peek().is_none()
    }
}

// impl<V> StreamSource<NoKey, V, usize> for SingleIteratorSource<V>
// where
//     V: Data,
// {
//     fn into_stream(
//         self,
//         name: &str,
//         builder: JetStreamBuilder<NoKey, NoData, NoTime>,
//     ) -> JetStreamBuilder<NoKey, V, usize> {
//         let operator = OperatorBuilder::built_by(name, |build_context| {
//             let mut inner = if build_context.worker_id == 0 {
//                 self.iterator.enumerate()
//             } else {
//                 // do not emit on non-0 worker
//                 (Box::new(iter::empty::<V>()) as Box<dyn Iterator<Item = V>>).enumerate()
//             };
//             let mut final_emitted = false;

//             move |input: &mut Input<NoKey, NoData, NoTime>,
//                   output: &mut Output<NoKey, V, usize>,
//                   _ctx| {
//                 if !final_emitted {
//                     if let Some(x) = inner.next() {
//                         output.send(Message::Data(DataMessage::new(NoKey, x.1, x.0)));
//                     } else {
//                         output.send(Message::Epoch(usize::MAX));
//                         final_emitted = true;
//                     }
//                 }
//                 if let Some(msg) = input.recv() {
//                     match msg {
//                         Message::Data(_) => (),
//                         Message::Epoch(_) => (),
//                         Message::AbsBarrier(x) => output.send(Message::AbsBarrier(x)),
//                         // Message::Load(x) => output.send(Message::Load(x)),
//                         Message::Rescale(x) => output.send(Message::Rescale(x)),
//                         Message::SuspendMarker(x) => output.send(Message::SuspendMarker(x)),
//                         Message::Interrogate(x) => output.send(Message::Interrogate(x)),
//                         Message::Collect(x) => output.send(Message::Collect(x)),
//                         Message::Acquire(x) => output.send(Message::Acquire(x)),
//                     }
//                 }
//             }
//         });
//         builder.then(operator)
//     }
// }

#[cfg(test)]
mod tests {

    use itertools::Itertools;
    use proptest::bits::usize;

    use crate::{
        operators::*,
        runtime::{threaded::MultiThreadRuntime, WorkerBuilder},
        sinks::StatelessSink,
        snapshot::{NoPersistence, NoSnapshots},
        sources::{SingleIteratorSource, StatelessSource},
        testing::{get_test_stream, VecSink},
        types::Message,
    };

    /// The into_iter source should emit the iterator values
    #[test]
    fn emits_values() {
        let (builder, stream) = get_test_stream();

        let in_data: Vec<i32> = (0..100).collect();
        let collector = VecSink::new();

        let stream = stream
            .source(
                "source",
                StatelessSource::new(SingleIteratorSource::new(in_data)),
            )
            .sink("sink", StatelessSink::new(collector.clone()));
        builder.build().unwrap().0.execute();

        let c = collector.into_iter().map(|x| x.value).collect_vec();
        assert_eq!(c, (0..100).collect_vec())
    }

    // /// It should only emit records on worker 0 to avoid duplicates
    // #[test]
    // fn emits_only_on_worker_0() {
    //     let sink = VecSink::new();

    //     let args = [sink.clone(), sink.clone()];

    //     let rt = MultiThreadRuntime::new_with_args(
    //         |flavor, this_sink| {
    //             let mut builder = WorkerBuilder::new(flavor, NoSnapshots, NoPersistence::default());
    //             builder
    //                 .new_stream()
    //                 .source(
    //                     "source",
    //                     StatelessSource::new(SingleIteratorSource::new(0..10)),
    //                 )
    //                 .sink("sink", StatelessSink::new(this_sink));
    //             builder
    //         },
    //         args,
    //     );

    //     rt.execute().unwrap();
    //     // if both threads emitted values, we would expect this to contain 20 values, not 10
    //     assert_eq!(sink.len(), 10);
    // }

    /// values should be timestamped with their iterator index
    #[test]
    fn emits_timestamped_messages() {
        let (builder, stream) = get_test_stream();
        let sink = VecSink::new();
        stream
            .source(
                "source",
                StatelessSource::new(SingleIteratorSource::new(42..52)),
            )
            .sink("sink", StatelessSink::new(sink.clone()));
        builder.build().unwrap().0.execute();

        let timestamps = sink.into_iter().map(|x| x.timestamp).collect_vec();
        let expected = (0..10).collect_vec();
        assert_eq!(expected, timestamps);
    }

    /// after the final value a single usize::MAX epoch should
    /// be emitted
    #[test]
    fn emits_max_epoch() {
        let (builder, stream) = get_test_stream();
        let sink = VecSink::new();
        stream
            .source(
                "source",
                StatelessSource::new(SingleIteratorSource::new(0..10)),
            )
            .sink_full("sink", sink.clone());
        builder.build().unwrap().0.execute();

        let messages = sink.drain_vec(..);
        let last = messages.last().unwrap();
        match last {
            Message::Epoch(usize::MAX) => (),
            _ => panic!(),
        }
    }
}
