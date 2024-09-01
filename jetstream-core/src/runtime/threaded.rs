mod single;
mod communication;

pub use single::SingleThreadRuntime;
pub use communication::{InterThreadCommunication, InterThreadCommunicationError};
pub(crate) use communication::Shared;