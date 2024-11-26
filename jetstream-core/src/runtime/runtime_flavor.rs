use super::communication::CommunicationBackend;

pub trait RuntimeFlavor {
    /// The type of backend this runtime uses for inter-worker communication
    type Communication: CommunicationBackend;

    /// Establish communication between multiple JetStream workers,
    /// possibly on different machines
    fn establish_communication(&mut self) -> Result<Self::Communication, CommunicationError>;

    /// The size of this compute cluster i.e. the number of JetStream workers
    /// at the time this method is called.
    /// The number returned should **include** the worker from which this
    /// method was called
    fn runtime_size(&self) -> u64;

    /// Return the ID of the worker where this method was called
    fn this_worker_id(&self) -> u64;
}

/// Error to be returned if a communication could not be established
#[derive(thiserror::Error, Debug)]
#[error(transparent)]
pub struct CommunicationError(#[from] Box<dyn std::error::Error>);

impl CommunicationError {
    pub fn from_error<E>(err: E) -> Self
    where
        E: std::error::Error + 'static,
    {
        Self(Box::new(err))
    }
}
