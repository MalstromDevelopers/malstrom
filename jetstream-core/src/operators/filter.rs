use super::stateless_op::StatelessOp;
use crate::stream::jetstream::JetStreamBuilder;
use crate::time::Timestamp;
use crate::{Data, MaybeKey};

pub trait Filter<K, V, T> {
    /// Filters the datastream based on a given predicate.
    ///
    /// The given function receives an immutable reference to the value
    /// of every data message reaching this operator.
    /// If the function return `true`, the message will be retained and
    /// passed downstream, if the function returns `false`, the message
    /// will be dropped.
    ///
    /// # Example
    ///
    /// Only retain numbers <= 42
    /// ```
    /// stream: JetStreamBuilder<NoKey, i64, NoTime, NoPersistence>
    /// stream.filter(|x| *x <= 42)
    /// ````
    fn filter(self, filter: impl FnMut(&V) -> bool + 'static) -> JetStreamBuilder<K, V, T>;
}

impl<K, V, T> Filter<K, V, T> for JetStreamBuilder<K, V, T>
where
    K: MaybeKey,
    V: Data,
    T: Timestamp,
{
    fn filter(self, mut filter: impl FnMut(&V) -> bool + 'static) -> JetStreamBuilder<K, V, T> {
        self.stateless_op(move |item, out| {
            if filter(&item.value) {
                out.send(crate::Message::Data(item))
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        operators::{sink::Sink, source::Source},
        sources::SingleIteratorSource,
        test::{get_test_stream, VecCollector},
    };

    use super::*;
    #[test]
    fn test_filter() {
        let (builder, stream) = get_test_stream();

        let collector = VecCollector::new();

        let stream = stream
            .source(SingleIteratorSource::new(0..100))
            .filter(|x| *x < 42)
            .sink(collector.clone());
        stream.finish();
        let mut worker = builder.build().unwrap();

        worker.execute();

        let collected: Vec<usize> = collector.into_iter().map(|x| x.value).collect();
        let expected: Vec<usize> = (0..42).collect();
        assert_eq!(expected, collected)
    }
}
