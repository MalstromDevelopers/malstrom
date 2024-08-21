mod builder;
mod threaded;
pub mod communication;
mod runtime_flavor;
mod execution_handle;

pub use threaded::{SingleThreadRuntime, SingleThreadExecution};
pub use runtime_flavor::RuntimeFlavor;
pub use communication::{CommunicationBackend, CommunicationClient};
pub use builder::{RuntimeBuilder, Worker};

pub(crate) use builder::{InnerRuntimeBuilder, split_n, union};