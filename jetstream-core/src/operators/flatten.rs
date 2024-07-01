use super::stateless_op::StatelessOp;
use crate::stream::jetstream::JetStreamBuilder;
use crate::time::MaybeTime;
use crate::{Data, DataMessage, MaybeKey};

pub trait Flatten<K, VI, T, VO, I> {
    /// Flatten a datastream. Given a stream of some iterables, this function consumes
    /// each iterable and emits each of its elements downstream.
    /// 
    /// # Key and Time
    /// If the message containing the iterator has a key or timestamp,
    /// they are cloned and attached to every emitted message.
    /// 
    /// # Example
    ///
    /// Only retain numbers <= 42
    /// ```
    /// stream: JetStreamBuilder<NoKey, Vec<i64>, NoTime, NoPersistence>
    /// let flat: JetStreamBuilder<NoKey, i64, NoTime, NoPersistence> = stream.flatten();
    /// 
    /// ````
    fn flatten(self) -> JetStreamBuilder<K, VO, T>;
}

impl<K, VI, T, VO, I> Flatten<K, VI, T, VO, I> for JetStreamBuilder<K, VI, T>
where
    K: MaybeKey,
    I: Iterator<Item = VO>,
    VI: IntoIterator<Item=VO, IntoIter=I> + Data,
    VO: Data,
    T: MaybeTime
{
    fn flatten(
        self,
    ) -> JetStreamBuilder<K, VO, T> {
        self.stateless_op(move |item, out| {
            let key = item.key;
            let timestamp = item.timestamp;
            for x in item.value {
                out.send(crate::Message::Data(DataMessage::new(key.clone(), x, timestamp.clone())))
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

    use super::Flatten;
    #[test]
    fn test_filter() {
        let [config] = get_test_configs();
        let (mut worker, stream) = get_test_stream();

        let collector = VecCollector::new();

        let stream = stream
            .source([vec![1, 2], vec![3, 4], vec![5]])
            .flatten()
            .sink(collector.clone());

        worker.add_stream(stream);
        let mut runtime = worker.build(config).unwrap();

        while collector.len() < 5 {
            runtime.step()
        }
        let collected: Vec<usize> = collector
            .drain_vec(..)
            .into_iter()
            .map(|x| x.value)
            .collect();
        let expected: Vec<usize> = (0..6).collect();
        assert_eq!(expected, collected)
    }
}
