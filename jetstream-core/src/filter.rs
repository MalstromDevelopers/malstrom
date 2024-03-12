use crate::Data;

use crate::snapshot::PersistenceBackend;
use crate::stateless_op::StatelessOp;
use crate::stream::jetstream::JetStreamBuilder;
use crate::time::Timestamp;
use crate::Key;

pub trait Filter<K, V, T, P> {
    fn filter(self, filter: impl FnMut(&V) -> bool + 'static) -> JetStreamBuilder<K, V, T, P>;
}

impl<K, V, T, P> Filter<K, V, T, P> for JetStreamBuilder<K, V, T, P>
where
    K: Key,
    V: Data,
    T: Timestamp,
    P: PersistenceBackend,
{
    fn filter(self, mut filter: impl FnMut(&V) -> bool + 'static) -> JetStreamBuilder<K, V, T, P> {
        self.stateless_op(move |item, out| {
            if filter(&item.value) {
                out.send(crate::Message::Data(item))
            }
        })
    }
}
