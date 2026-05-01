use serde::{Deserialize, Serialize};

use crate::{
    keyed::distributed::{ConfigVersion, wire_message::WireMessage},
    types::{DataMessage, Kvt, Message, WorkerId},
};

/// A wrapper around a Malstrom message which includes the Sender WorkerId and Version
/// NOTE: For the local worker the version ID is always 0
#[derive(Clone)]
pub(super) enum TargetedMessage<M: Kvt> {
    Data(TargetedData<M>),
    /// Guaranteed to not be Message::Data
    Other(Message<M>),
}

impl<M: Kvt> TargetedMessage<M> {
    pub(super) fn from_local_msg(
        msg: Message<M>,
        target_id: WorkerId,
        config_version: ConfigVersion,
    ) -> Self {
        match msg {
            Message::Data(d) => Self::Data(TargetedData {
                target_id,
                config_version,
                data_msg: d,
            }),
            x => Self::Other(x),
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(bound(
    serialize = "M::Key: Serialize, M::Value: Serialize, M::Timestamp: Serialize",
    deserialize = "M::Key: Deserialize<'de>, M::Value: Deserialize<'de>, M::Timestamp: Deserialize<'de>"
))]
pub(super) struct TargetedData<M: Kvt> {
    pub(super) target_id: WorkerId,
    pub(super) config_version: ConfigVersion,
    pub(super) data_msg: DataMessage<M>,
}
