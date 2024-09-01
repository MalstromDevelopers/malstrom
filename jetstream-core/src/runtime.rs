mod builder;
pub mod threaded;
pub mod communication;
mod runtime_flavor;

pub use runtime_flavor::RuntimeFlavor;
pub use communication::{CommunicationBackend, CommunicationClient};
pub use builder::{RuntimeBuilder, Worker};

pub(crate) use builder::{InnerRuntimeBuilder, split_n, union};
