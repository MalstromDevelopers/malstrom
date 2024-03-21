use super::stateless_op::StatelessOp;
use crate::stream::jetstream::JetStreamBuilder;
use crate::time::MaybeTime;
use crate::{Data, DataMessage, MaybeKey};

pub trait FlatMap<K, V, T, VO, P> {
    fn flat_map(
        self,
        mapper: impl (FnMut(V) -> Vec<VO>) + 'static,
    ) -> JetStreamBuilder<K, VO, T, P>;
}

impl<K, V, T, VO, P> FlatMap<K, V, T, VO, P> for JetStreamBuilder<K, V, T, P>
where
    K: MaybeKey,
    V: Data,
    VO: Data,
    T: MaybeTime,
    P: 'static,
{
    fn flat_map(
        self,
        mut mapper: impl (FnMut(V) -> Vec<VO>) + 'static,
    ) -> JetStreamBuilder<K, VO, T, P> {
        self.stateless_op(move |item, out| {
            let k = item.key;
            let t = item.timestamp;
            for x in mapper(item.value) {
                out.send(crate::Message::Data(DataMessage::new(
                    k.clone(),
                    x,
                    t.clone(),
                )))
            }
        })
    }
}
