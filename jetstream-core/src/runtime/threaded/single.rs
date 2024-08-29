use crate::runtime::{execution_handle::ExecutionHandle, CommunicationBackend, RuntimeFlavor, Worker};

use super::{communication::InterThreadCommunication, Shared};

/// Runs all dataflows in a single thread on a
/// single machine with no parrallelism.
#[derive(Debug, Default)]
pub struct SingleThreadRuntime;

impl RuntimeFlavor for SingleThreadRuntime {
    type Communication = InterThreadCommunication;
    type ExecutionHandle = SingleThreadExecution<Self::Communication>;
    
    fn establish_communication(&mut self) -> Result<Self::Communication, crate::runtime::runtime_flavor::CommunicationError> {
        Ok(InterThreadCommunication::new(Shared::default(), 0))
    }
    
    fn runtime_size(&self) -> usize {
        1
    }
    
    fn this_worker_id(&self) -> usize {
        0
    }
    
    fn create_handle(self, worker: crate::runtime::Worker<Self::Communication>) -> Self::ExecutionHandle {
        SingleThreadExecution{worker}
    }
}


pub struct SingleThreadExecution<C>{
    worker: Worker<C>
}
impl <C> SingleThreadExecution<C> {
    pub(crate) fn step(&self) {
        todo!()
    }
}
impl <C> ExecutionHandle for SingleThreadExecution<C> where C: CommunicationBackend{
    fn execute(mut self) -> Result<(), crate::runtime::execution_handle::ExecutionError> {
        self.worker.execute();
        Ok(())
    }
}
