use super::communication::{OperatorOperatorComm, WorkerCoordinatorComm};

/// A specific implementation of a runtime. A runtime is anything, where a Malstrom job can be
/// executed, for example the [MultiThreadRuntime](super::threaded::MultiThreadRuntime)
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
    /// Create a CommunicationError from any type implementing [std::error::Error]
    pub fn from_error<E>(err: E) -> Self
    where
        E: std::error::Error + 'static + Send + Sync,
    {
        Self(Box::new(err))
    }
}
