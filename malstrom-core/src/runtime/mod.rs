mod builder;
pub mod communication;
mod runtime_flavor;
pub mod threaded;
// mod rescaling;

pub use builder::{StreamProvider, WorkerBuilder};
pub use communication::{BiCommunicationClient, CommunicationClient, OperatorOperatorComm};
pub use runtime_flavor::{CommunicationError, RuntimeFlavor};
pub use threaded::{MultiThreadRuntime, SingleThreadRuntime, SingleThreadRuntimeFlavor};

pub(crate) use builder::{union, InnerRuntimeBuilder};
