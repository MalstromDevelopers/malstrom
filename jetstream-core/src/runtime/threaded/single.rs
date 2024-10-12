use crate::runtime::RuntimeFlavor;

use super::{communication::InterThreadCommunication, Shared};

/// Runs all dataflows in a single thread on a
/// single machine with no parrallelism.
#[derive(Debug, Default)]
pub(crate) struct SingleThreadRuntime;

impl RuntimeFlavor for SingleThreadRuntime {
    type Communication = InterThreadCommunication;
    
    fn establish_communication(&mut self) -> Result<Self::Communication, crate::runtime::runtime_flavor::CommunicationError> {
        Ok(InterThreadCommunication::new(Shared::default(), 0))
    }
    
    fn runtime_size(&self) -> usize {
        1
    }
    
    fn this_worker_id(&self) -> usize {
        0
    }
}
