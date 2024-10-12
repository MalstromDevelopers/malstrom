mod single;
mod multi;
mod communication;

pub(crate) use single::SingleThreadRuntime;
pub use multi::MultiThreadRuntime;
pub use communication::{InterThreadCommunication, InterThreadCommunicationError};
pub(crate) use communication::Shared;