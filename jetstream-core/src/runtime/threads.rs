use std::convert::Infallible;

use crate::keyed::distributed::Distributable;

use super::{
    communication::CommunicationBackend,
    runtime_flavor::{CommunicationError, RuntimeFlavor},
};

/// Runs all dataflows in a single thread on a
/// single machine with no parrallelism.
#[derive(Default)]
pub struct SingleThreadRuntime;

impl RuntimeFlavor for SingleThreadRuntime {
    type Communication = TodoCommunication;
    fn establish_communication(&mut self) -> Result<Self::Communication, CommunicationError> {
        todo!()
    }

    fn runtime_size(&self) -> usize {
        1
    }

    fn this_worker_id(&self) -> usize {
        0
    }
}

/// Runs JetStream dataflows on one or more threads on
/// a single machine.
pub struct MultiThreadRuntime {
    thread_count: usize,
}

// impl RuntimeFlavor for MultiThreadRuntime {
//     fn establish_communication(&mut self) -> impl CommunicationBackend {
//         todo!()
//     }
// }

struct TodoCommunication;

impl CommunicationBackend for TodoCommunication {
    fn new_connection(
        &mut self,
        to_worker: crate::WorkerId,
        to_operator: crate::OperatorId,
        from: crate::OperatorId,
    ) -> Result<Box<dyn super::communication::Transport>, super::communication::ClientBuildError>
    {
        todo!()
    }
}

/// A CommunicationBackend for threaded runtimes
/// utilizing channels for inter-thread communication
struct InterThreadCommunication;

impl InterThreadCommunication {
    fn new() -> Self {
        InterThreadCommunication
    }
}
impl CommunicationBackend for InterThreadCommunication {
    fn new_connection(
        &mut self,
        to_worker: crate::WorkerId, to_operator: crate::OperatorId,
        from: crate::OperatorId,
    ) -> Result<Box<dyn super::communication::Transport>, super::communication::ClientBuildError> {
        todo!()
    }
}

struct InterThreadTransport;

#[cfg(test)]
mod test {
    use super::InterThreadCommunication;

    /// check we can send and recv messages via thread communication
    fn test_thread_communication_send_recv() {
        let a = InterThreadCommunication::new();
        let b = InterThreadCommunication::new();


    }
}
