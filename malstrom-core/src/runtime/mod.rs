//! Runtimes determine where Malstrom jobs are executed. Runtimes provide the necessary
//! infrastructure and communication channels for Malstrom to run jobs.
pub mod communication;
pub(crate) mod runtime_flavor;
pub mod threaded;

pub use communication::OperatorOperatorComm;
pub use runtime_flavor::RuntimeFlavor;
pub use threaded::{SingleThreadRuntime, SingleThreadRuntimeFlavor};
