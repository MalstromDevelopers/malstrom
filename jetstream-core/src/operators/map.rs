use super::stateless_op::StatelessOp;
use crate::stream::jetstream::JetStreamBuilder;
use crate::stream::operator_trait::OperatorTrait;
use crate::time::MaybeTime;
use crate::{Data, DataMessage, MaybeKey};

pub trait Map<K, V, T, VO> {
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
