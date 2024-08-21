use crate::{runtime::CommunicationBackend, OperatorId, WorkerId};
use std::sync::{mpsc::{channel, Receiver, Sender}, Arc, Mutex};
use indexmap::IndexMap;

/// contains the sender/recv to be used to send messages to the operator
/// at (WorkerId, OperatorId)
/// The receiving operator will take the Receiver as soon as it starts listening, leaving
/// a None.
/// If another client starts sending before there is a listener, there will be a Some()
type AddressMap = IndexMap<(WorkerId, OperatorId), (Sender<Vec<u8>>, Option<Receiver<Vec<u8>>>)>;
type Shared = Arc<Mutex<AddressMap>>;

/// Provides simple inter-thread communication via channels
pub struct InterThreadCommunication{
    shared: Shared
}
impl InterThreadCommunication {
    pub(super) fn new(shared: Shared) -> Self {
        Self { shared }
    }
}

/// MPSC Channel based transport
struct ChannelTransport;

impl CommunicationBackend for InterThreadCommunication {
    fn new_connection(
        &mut self,
        to_worker: crate::WorkerId, to_operator: crate::OperatorId,
        from: crate::OperatorId,
    ) -> Result<Box<dyn crate::runtime::communication::Transport>, crate::runtime::communication::ClientBuildError> {
        todo!()
    }
}

#[cfg(test)]
mod test {
    use std::sync::{Arc, Mutex};

    use super::Shared;

    /// check we can send and recv a single message
    fn send_recv_message() {
        let shared = Shared::default();
    }
}