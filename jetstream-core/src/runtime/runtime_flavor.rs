use crate::WorkerId;

use super::{communication::CommunicationBackend, execution_handle::ExecutionHandle, Worker};

pub trait RuntimeFlavor {
    /// The type of backend this runtime uses for inter-worker communication
    type Communication: CommunicationBackend;

    /// the type of ExecutionHandle this Runtime gives to the user
    type ExecutionHandle: ExecutionHandle;

    /// Establish communication between multiple JetStream workers,
    /// possibly on different machines
    fn establish_communication(&mut self) -> Result<Self::Communication, CommunicationError>;

    /// The size of this compute cluster i.e. the number of JetStream workers
    /// at the time this method is called.
    /// The number returned should **include** the worker from which this
    /// method was called
    fn runtime_size(&self) -> usize;

    /// Return the ID of the worker where this method was called
    fn this_worker_id(&self) -> usize;

    fn create_handle(self, worker: Worker<Self::Communication>) -> Self::ExecutionHandle;
}

/// Error to be returned if a communication could not be established
#[derive(thiserror::Error, Debug)]
#[error(transparent)]
pub struct CommunicationError(#[from] Box<dyn std::error::Error>);