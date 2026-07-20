use serde::{Deserialize, Serialize};

use crate::{
    keyed::distributed::{ConfigVersion, routers::RouterOutput, wire_message::WireMessage},
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

impl<M> From<RouterOutput<M>> for TargetedMessage<M>
where
    M: Kvt,
{
    fn from(value: RouterOutput<M>) -> Self {
        match value {
            RouterOutput::DataMessage(targeted_data) => TargetedMessage::Data(targeted_data),
            RouterOutput::Rescale(rescale_message) => {
                TargetedMessage::Other(Message::Rescale(rescale_message))
            }
            RouterOutput::Complete(reconfig_complete) => {
                TargetedMessage::Other(Message::ReconfigComplete(reconfig_complete))
            }
            RouterOutput::Collect(collect) => TargetedMessage::Other(Message::Collect(collect)),
            RouterOutput::Acquire(wire_acquire) => {
                TargetedMessage::Other(Message::Acquire(wire_acquire.into()))
            }
            RouterOutput::Interrogate(interrogate) => {
                TargetedMessage::Other(Message::Interrogate(interrogate))
            }
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
