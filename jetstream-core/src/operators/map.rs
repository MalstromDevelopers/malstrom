use super::stateless_op::StatelessOp;
use crate::stream::jetstream::JetStreamBuilder;
use crate::time::MaybeTime;
use crate::{Data, DataMessage, MaybeKey};

pub trait Map<K, V, T, VO, P> {
    fn map(self, mapper: impl (FnMut(V) -> VO) + 'static) -> JetStreamBuilder<K, VO, T, P>;
}

impl<K, V, T, VO, P> Map<K, V, T, VO, P> for JetStreamBuilder<K, V, T, P>
where
    K: MaybeKey,
    V: Data,
    VO: Data,
    T: MaybeTime,
    P: 'static,
{
    fn map(self, mut mapper: impl (FnMut(V) -> VO) + 'static) -> JetStreamBuilder<K, VO, T, P> {
        self.stateless_op(move |item, out| {
            out.send(crate::Message::Data(DataMessage::new(
                item.key,
                mapper(item.value),
                item.timestamp,
            )));
        })
    }
}
