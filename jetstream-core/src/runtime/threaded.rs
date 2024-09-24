mod single;
mod multi;
mod communication;

pub use single::SingleThreadRuntime;
pub use multi::MultiThreadRuntime;
pub use communication::{InterThreadCommunication, InterThreadCommunicationError};
pub(crate) use communication::Shared;