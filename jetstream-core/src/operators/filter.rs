use super::stateless_op::StatelessOp;
use crate::stream::jetstream::JetStreamBuilder;
use crate::time::MaybeTime;
use crate::{Data, MaybeKey};

pub trait Filter<K, V, T, P> {
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
    fn filter(self, filter: impl FnMut(&V) -> bool + 'static) -> JetStreamBuilder<K, V, T, P>;
}

impl<K, V, T, P> Filter<K, V, T, P> for JetStreamBuilder<K, V, T, P>
where
    K: MaybeKey,
    V: Data,
    T: MaybeTime,
    P: 'static,
{
    fn filter(self, mut filter: impl FnMut(&V) -> bool + 'static) -> JetStreamBuilder<K, V, T, P> {
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
        test::{get_test_configs, get_test_stream, VecCollector},
    };

    use super::*;
    #[test]
    fn test_filter() {
        let [config] = get_test_configs();
        let (mut worker, stream) = get_test_stream();

        let collector = VecCollector::new();

        let stream = stream
            .source(0..100)
            .filter(|x| *x < 42)
            .sink(collector.clone());

        worker.add_stream(stream);
        let mut runtime = worker.build(config).unwrap();

        while collector.len() < 42 {
            runtime.step()
        }
        let collected: Vec<usize> = collector
            .drain_vec(..)
            .into_iter()
            .map(|x| x.value)
            .collect();
        let expected: Vec<usize> = (0..42).collect();
        assert_eq!(expected, collected)
    }
}
