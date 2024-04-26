use derive_new::new;
use serde::{Deserialize, Serialize};

use crate::{
    stream::operator::{BuildContext, Logic},
    DataMessage, MaybeData, MaybeKey, Message, WorkerId,
};

use super::{DistTimestamp, Version};

#[derive(Debug, Clone, Serialize, Deserialize, new)]
pub(crate) struct VersionedMessage<V> {
    pub(super) inner: V,
    pub(super) version: Version,
    pub(super) sender: WorkerId,
}

/// Builds an operator that creates versioned messages. Note that this is
/// just a type converter, which always issues messages with version 0
pub(crate) fn versioner<K: MaybeKey, V: MaybeData, T: DistTimestamp>(
    ctx: &mut BuildContext,
) -> impl Logic<K, V, T, K, VersionedMessage<V>, T> {
    let wid = ctx.worker_id;
    move |input, output, _ctx| {
        if let Some(msg) = input.recv() {
            match msg {
                Message::Data(d) => {
                    let vmsg = VersionedMessage {
                        inner: d.value,
                        version: 0,
                        sender: wid,
                    };
                    output.send(Message::Data(DataMessage::new(d.key, vmsg, d.timestamp)))
                }
                Message::Epoch(e) => output.send(Message::Epoch(e)),
                Message::AbsBarrier(b) => output.send(Message::AbsBarrier(b)),
                Message::Rescale(r) => output.send(Message::Rescale(r)),
                Message::ShutdownMarker(s) => output.send(Message::ShutdownMarker(s)),
                Message::Interrogate(i) => output.send(Message::Interrogate(i)),
                Message::Collect(c) => output.send(Message::Collect(c)),
                Message::Acquire(a) => output.send(Message::Acquire(a)),
                Message::DropKey(d) => output.send(Message::DropKey(d)),
            }
        }
    }
}
