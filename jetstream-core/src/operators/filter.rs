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
    /// Only retain numbers bigger >= 42
    /// ```
    /// let stream = Worker::test_local().new_stream()
    /// let values = vec![1337, 36, 7, 42]
    /// let collected = Vec::new()
    ///
    /// (stream
    ///     .source(values)
    ///     .filter(|x| x >= 42)
    ///     .sink(&mut collected)
    ///     .run_to_completion()
    /// );
    ///
    /// assert_eq!(collected, vec![1337, 42])
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
