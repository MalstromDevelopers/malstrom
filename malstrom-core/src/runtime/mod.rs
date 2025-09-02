//! Runtimes determine where Malstrom jobs are executed. Runtimes provide the necessary
//! infrastructure and communication channels for Malstrom to run jobs.
pub mod communication;
pub(crate) mod runtime_flavor;
pub mod threaded;

pub use communication::{BiCommunicationClient, CommunicationClient, OperatorOperatorComm};
pub use runtime_flavor::{CommunicationError, RuntimeFlavor};
pub use threaded::{MultiThreadRuntime, SingleThreadRuntime, SingleThreadRuntimeFlavor};
