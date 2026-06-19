use super::communication::{OperatorOperatorComm, WorkerCoordinatorComm};

/// A specific implementation of a runtime. A runtime is anything, where a Malstrom job can be
/// executed, for example the [MultiThreadRuntime](super::threaded::MultiThreadRuntime)
pub trait RuntimeFlavor {
    /// The type of backend this runtime uses for inter-worker communication
    type Communication: OperatorOperatorComm + WorkerCoordinatorComm + 'static;

    /// Establish communication between multiple JetStream workers,
    /// possibly on different machines
    fn communication(
        &mut self,
    ) -> Result<Self::Communication, Box<dyn std::error::Error + Send + Sync>>;

    /// Return the ID of the worker where this method was called
    fn this_worker_id(&self) -> u64;
}
