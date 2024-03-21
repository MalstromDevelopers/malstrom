use indexmap::IndexSet;

use crate::{
    snapshot::{Barrier, Load},
    time::Epoch,
    DataMessage, Message, ShutdownMarker, WorkerId,
};
/// Same as the normal jetstream messages but without Epochs, Data,
/// or key related messages, since those can not reach the source.
#[derive(Debug)]
pub enum SystemMessage<P> {
    /// Barrier used for asynchronous snapshotting
    AbsBarrier(Barrier<P>),
    /// Instruction to load state
    Load(Load<P>),
    /// Information that the worker of this ID will soon be removed
    /// from the computation. Triggers Rescaling procedure
    ScaleRemoveWorker(IndexSet<WorkerId>),
    /// Information that the worker of this ID will be added to the
    /// computation. Triggers Rescaling procedure
    ScaleAddWorker(IndexSet<WorkerId>),
    /// Information that this worker plans on shutting down
    /// See struct docstring for more information
    ShutdownMarker(ShutdownMarker),
}

// #[derive(Debug, Clone)]
// pub(super) struct NotSystemMessage<K, V, T, P>(pub Message<K, V, T, P>);

impl<K, V, T, P> TryFrom<Message<K, V, T, P>> for SystemMessage<P> {
    type Error = Message<K, V, T, P>;
    fn try_from(value: Message<K, V, T, P>) -> Result<Self, Self::Error> {
        match value {
            Message::AbsBarrier(x) => Ok(Self::AbsBarrier(x)),
            // Message::Load(x) => Ok(Self::Load(x)),
            Message::ScaleRemoveWorker(x) => Ok(Self::ScaleRemoveWorker(x)),
            Message::ScaleAddWorker(x) => Ok(Self::ScaleAddWorker(x)),
            Message::ShutdownMarker(x) => Ok(Self::ShutdownMarker(x)),
            x => Err(x),
        }
    }
}
impl<K, V, T, P> From<SystemMessage<P>> for Message<K, V, T, P> {
    fn from(value: SystemMessage<P>) -> Message<K, V, T, P> {
        match value {
            SystemMessage::AbsBarrier(x) => Message::AbsBarrier(x),
            SystemMessage::Load(x) => todo!(), // Message::Load(x),
            SystemMessage::ScaleRemoveWorker(x) => Message::ScaleRemoveWorker(x),
            SystemMessage::ScaleAddWorker(x) => Message::ScaleAddWorker(x),
            SystemMessage::ShutdownMarker(x) => Message::ShutdownMarker(x),
        }
    }
}

/// Only epochs and data
#[derive(Debug)]
pub enum SourceMessage<K, V, T> {
    Data(DataMessage<K, V, T>),
    Epoch(Epoch<T>),
}

impl<K, V, T, P> From<SourceMessage<K, V, T>> for Message<K, V, T, P> {
    fn from(value: SourceMessage<K, V, T>) -> Message<K, V, T, P> {
        match value {
            SourceMessage::Data(x) => Message::Data(x),
            SourceMessage::Epoch(x) => Message::Epoch(x),
        }
    }
}
