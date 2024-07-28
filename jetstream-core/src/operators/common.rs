use crate::{DataMessage, Message};
/// Same as the normal jetstream messages but without Epochs, Data,
/// or key related messages, since those can not reach the source.
// #[derive(Debug)]
// pub enum SystemMessage {
//     /// Barrier used for asynchronous snapshotting
//     AbsBarrier(Barrier),
//     /// Instruction to load state
//     Load(Load),
//     /// Information that the worker of this ID will soon be removed
//     /// from the computation. Triggers Rescaling procedure
//     ScaleRemoveWorker(IndexSet<WorkerId>),
//     /// Information that the worker of this ID will be added to the
//     /// computation. Triggers Rescaling procedure
//     ScaleAddWorker(IndexSet<WorkerId>),
//     /// Information that this worker plans on shutting down
//     /// See struct docstring for more information
//     ShutdownMarker(ShutdownMarker),
// }

// // #[derive(Debug, Clone)]
// // pub(super) struct NotSystemMessage<K, V, T>(pub Message<K, V, T>);

// impl<K, V, T> TryFrom<Message<K, V, T>> for SystemMessage {
//     type Error = Message<K, V, T>;
//     fn try_from(value: Message<K, V, T>) -> Result<Self, Self::Error> {
//         match value {
//             Message::AbsBarrier(x) => Ok(Self::AbsBarrier(x)),
//             // Message::Load(x) => Ok(Self::Load(x)),
//             Message::Rescale(x) => Ok(Self::Rescale(x)),
//             Message::ShutdownMarker(x) => Ok(Self::ShutdownMarker(x)),
//             x => Err(x),
//         }
//     }
// }
// impl<K, V, T> From<SystemMessage> for Message<K, V, T> {
//     fn from(value: SystemMessage) -> Message<K, V, T> {
//         match value {
//             SystemMessage::AbsBarrier(x) => Message::AbsBarrier(x),
//             SystemMessage::Load(x) => todo!(), // Message::Load(x),
//             SystemMessage::ScaleRemoveWorker(x) => Message::ScaleRemoveWorker(x),
//             SystemMessage::ScaleAddWorker(x) => Message::ScaleAddWorker(x),
//             SystemMessage::ShutdownMarker(x) => Message::ShutdownMarker(x),
//         }
//     }
// }

/// Only epochs and data
#[derive(Debug)]
pub enum SourceMessage<K, V, T> {
    Data(DataMessage<K, V, T>),
    Epoch(T),
}

impl<K, V, T> From<SourceMessage<K, V, T>> for Message<K, V, T> {
    fn from(value: SourceMessage<K, V, T>) -> Message<K, V, T> {
        match value {
            SourceMessage::Data(x) => Message::Data(x),
            SourceMessage::Epoch(x) => Message::Epoch(x),
        }
    }
}
