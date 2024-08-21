use crate::runtime::{execution_handle::ExecutionHandle, RuntimeFlavor};

use super::communication::InterThreadCommunication;

/// Runs all dataflows in a single thread on a
/// single machine with no parrallelism.
#[derive(Default)]
pub struct SingleThreadRuntime;


impl RuntimeFlavor for SingleThreadRuntime {
    type Communication = InterThreadCommunication;
    type ExecutionHandle = SingleThreadExecution;
    
    fn establish_communication(&mut self) -> Result<Self::Communication, crate::runtime::runtime_flavor::CommunicationError> {
        todo!()
    }
    
    fn runtime_size(&self) -> usize {
        1
    }
    
    fn this_worker_id(&self) -> usize {
        0
    }
    
    fn create_handle(self, worker: crate::runtime::Worker<Self::Communication>) -> Self::ExecutionHandle {
        todo!()
    }
}


pub struct SingleThreadExecution;
impl SingleThreadExecution {
    pub(crate) fn step(&self) {
        todo!()
    }
}
impl ExecutionHandle for SingleThreadExecution {
    fn execute(self) -> Result<(), crate::runtime::execution_handle::ExecutionError> {
        todo!()
    }
}
