use serde::{Deserialize, Serialize};

use crate::{
    keyed::distributed::{ConfigVersion, wire_message::WireMessage},
    types::{DataMessage, Kvt, Message, WorkerId},
};

/// A wrapper around a Malstrom message which includes the Sender WorkerId and Version
/// NOTE: For the local worker the version ID is always 0
#[derive(Clone)]
pub(super) enum VersionedMessage<M: Kvt> {
    Data(VersionedData<M>),
    /// Guaranteed to not be Message::Data
    Other(Message<M>)
}

impl<M: Kvt> VersionedMessage<M> {
    pub(super) fn from_local_msg(msg: Message<M>, this_worker: WorkerId) -> Self {
        match msg {
            Message::Data(d) => Self::Data(VersionedData { sender_id: this_worker, config_version: 0, data_msg: d }),
            x => Self::Other(x)
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(bound(
    serialize = "M::Key: Serialize, M::Value: Serialize, M::Timestamp: Serialize",
    deserialize = "M::Key: Deserialize<'de>, M::Value: Deserialize<'de>, M::Timestamp: Deserialize<'de>"
))]
pub(super) struct VersionedData<M: Kvt> {
    pub(super) sender_id: WorkerId,
    pub(super) config_version: ConfigVersion,
    pub(super) data_msg: DataMessage<M>
}