use super::stateless_op::StatelessOp;
use crate::stream::jetstream::JetStreamBuilder;
use crate::time::MaybeTime;
use crate::{Data, DataMessage, MaybeKey};

pub trait FlatMap<K, V, T, VO> {
    fn flat_map(self, mapper: impl (FnMut(V) -> Vec<VO>) + 'static) -> JetStreamBuilder<K, VO, T>;
}

impl<K, V, T, VO> FlatMap<K, V, T, VO> for JetStreamBuilder<K, V, T>
where
    K: MaybeKey,
    V: Data,
    VO: Data,
    T: MaybeTime,
{
    fn flat_map(
        self,
        mut mapper: impl (FnMut(V) -> Vec<VO>) + 'static,
    ) -> JetStreamBuilder<K, VO, T> {
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
