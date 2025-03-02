use super::communication::{CoordinatorWorkerComm, OperatorOperatorComm, WorkerCoordinatorComm};

pub trait RuntimeFlavor {
    /// The type of backend this runtime uses for inter-worker communication
    type Communication: OperatorOperatorComm + WorkerCoordinatorComm;

    /// Establish communication between multiple JetStream workers,
    /// possibly on different machines
    fn communication(&mut self) -> Result<Self::Communication, CommunicationError>;

    /// Return the ID of the worker where this method was called
    fn this_worker_id(&self) -> u64;
}

/// Error to be returned if a communication could not be established
#[derive(thiserror::Error, Debug)]
#[error(transparent)]
pub struct CommunicationError(#[from] Box<dyn std::error::Error + Send + Sync>);

impl CommunicationError {
    pub fn from_error<E>(err: E) -> Self
    where
        E: std::error::Error + 'static + Send + Sync,
    {
        Self(Box::new(err))
    }
}
