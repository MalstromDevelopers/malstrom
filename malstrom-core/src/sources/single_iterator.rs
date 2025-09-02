use std::iter::{Enumerate, Peekable};

use crate::types::{Data, NoKey};

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
pub struct SingleIteratorSource<T>(Option<Box<dyn Iterator<Item = T>>>);

impl<T> SingleIteratorSource<T> {
    /// Create a new source from an iterable value
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
        channels::operator_io::Input,
        operators::*,
        sinks::StatelessSink,
        sources::{SingleIteratorSource, StatelessSource},
        stream::OperatorBuilder,
        testing::get_test_rt,
        testing::VecSink,
        types::{Message, NoKey},
    };

    /// The into_iter source should emit the iterator values
    #[test]
    fn emits_values() {
        let in_data: Vec<i32> = (0..100).collect();
        let collector = VecSink::new();
        let rt = get_test_rt(|provider| {
            let in_data = in_data.clone();
            provider
                .new_stream()
                .source(
                    "source",
                    StatelessSource::new(SingleIteratorSource::new(in_data)),
                )
                .sink("sink", StatelessSink::new(collector.clone()));
        });
        rt.execute().unwrap();

        let c = collector.into_iter().map(|x| x.value).collect_vec();
        assert_eq!(c, (0..100).collect_vec())
    }

    /// values should be timestamped with their iterator index
    #[test]
    fn emits_timestamped_messages() {
        let sink = VecSink::new();
        let rt = get_test_rt(|provider| {
            provider
                .new_stream()
                .source(
                    "source",
                    StatelessSource::new(SingleIteratorSource::new(42..52)),
                )
                .sink("sink", StatelessSink::new(sink.clone()));
        });
        rt.execute().unwrap();

        let timestamps = sink.into_iter().map(|x| x.timestamp).collect_vec();
        let expected = (0..10).collect_vec();
        assert_eq!(expected, timestamps);
    }

    /// after the final value a single usize::MAX epoch should
    /// be emitted
    #[test]
    fn emits_max_epoch() {
        let sink = VecSink::new();
        let rt = get_test_rt(|provider| {
            let sink = sink.clone();
            provider
                .new_stream()
                .source(
                    "source",
                    StatelessSource::new(SingleIteratorSource::new(0..10)),
                )
                .then(OperatorBuilder::direct(
                    "sink-epochs",
                    move |input: &mut Input<NoKey, i32, usize>, output, _ctx| match input.recv() {
                        Some(Message::Epoch(x)) => {
                            sink.give(x.clone());
                            output.send(Message::Epoch(x));
                        }
                        Some(msg) => output.send(msg),
                        None => (),
                    },
                ));
        });
        rt.execute().unwrap();

        let messages = sink.drain_vec(..);
        let last = messages.last().unwrap();
        assert_eq!(*last, usize::MAX);
    }
}
