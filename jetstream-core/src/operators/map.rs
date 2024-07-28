use super::stateless_op::StatelessOp;
use crate::stream::jetstream::JetStreamBuilder;

use crate::time::MaybeTime;
use crate::{Data, DataMessage, MaybeKey};

pub trait Map<K, V, T, VO> {
    /// Map transforms every value in a datastream into a different value
    /// by applying a given function or closure.
    ///
    /// # Example
    /// ```
    /// stream: JetStreamBuilder<NoKey, &str, NoTime, NoPersistence>
    /// let lenghts = JetStreamBuilder<NoKey, usize, NoTime, NoPersistence> = stream.map(|x| x.len())
    /// ```
    fn map(self, mapper: impl (FnMut(V) -> VO) + 'static) -> JetStreamBuilder<K, VO, T>;
}

impl<K, V, T, VO> Map<K, V, T, VO> for JetStreamBuilder<K, V, T>
where
    K: MaybeKey,
    V: Data,
    VO: Data,
    T: MaybeTime,
{
    fn map(self, mut mapper: impl (FnMut(V) -> VO) + 'static) -> JetStreamBuilder<K, VO, T> {
        self.stateless_op(move |item, out| {
            out.send(crate::Message::Data(DataMessage::new(
                item.key,
                mapper(item.value),
                item.timestamp,
            )));
        })
    }
}
#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use crate::{
        operators::{
            map::Map,
            source::Source,
            timely::{GenerateEpochs, TimelyStream},
        },
        stream::jetstream::JetStreamBuilder,
        test::collect_stream_values,
    };

    #[test]
    fn test_map() {
        let input = ["hello", "world", "foo", "bar"];
        let output = input.iter().map(|x| x.len()).collect_vec();

        let stream = JetStreamBuilder::new_test()
            .source(input)
            .assign_timestamps(|_| 0)
            .generate_epochs(|x, _| {
                if x.value == "bar" {
                    Some(i32::MAX)
                } else {
                    None
                }
            })
            .0
            .map(|x| x.len());

        assert_eq!(collect_stream_values(stream), output);
    }
}
