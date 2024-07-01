use super::stateless_op::StatelessOp;
use crate::stream::jetstream::JetStreamBuilder;
use crate::time::MaybeTime;
use crate::{Data, DataMessage, MaybeKey, Message};

pub trait FilterMap<K, VI, T> {
    /// Applies a function to every element of the stream.
    /// All elements for which the function returns `Some(x)` are emitted downstream
    /// as `x`, all elements for which the function returns `None` are removed from
    /// the stream
    ///
    /// # Example
    ///
    /// Only retain numeric strings
    /// ```
    /// stream: JetStreamBuilder<NoKey, String, NoTime, NoPersistence>
    /// stream.filter_map(|x| x.parse::<i64>().ok()) 
    /// ````
    fn filter_map<VO: Data>(self, mapper: impl FnMut(VI) -> Option<VO> + 'static) -> JetStreamBuilder<K, VO, T>;
}

impl<K, VI, T> FilterMap<K, VI, T> for JetStreamBuilder<K, VI, T>
where
    K: MaybeKey,
    VI: Data,
    T: MaybeTime,
{
    fn filter_map<VO: Data>(self, mut mapper: impl FnMut(VI) -> Option<VO> + 'static) -> JetStreamBuilder<K, VO, T> {
        self.stateless_op(move |item, out| {
            if let Some(x) = mapper(item.value) {
                out.send(Message::Data(DataMessage::new(item.key, x, item.timestamp)))
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
    fn test_filter_map() {
        let [config] = get_test_configs();
        let (mut worker, stream) = get_test_stream();

        let collector = VecCollector::new();

        let stream = stream
            .source(["foobar", "42", "baz", "1337"])
            .filter_map(|x: &str| x.parse::<i64>().ok())
            .sink(collector.clone());

        worker.add_stream(stream);
        let mut runtime = worker.build(config).unwrap();

        while collector.len() < 2 {
            runtime.step()
        }
        let collected: Vec<i64> = collector
            .drain_vec(..)
            .into_iter()
            .map(|x| x.value)
            .collect();
        let expected: Vec<i64> = vec![42, 1337];
        assert_eq!(expected, collected)
    }
}
