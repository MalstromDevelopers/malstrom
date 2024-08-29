mod single;
mod communication;

pub use single::{SingleThreadRuntime, SingleThreadExecution};
pub use communication::{InterThreadCommunication, InterThreadCommunicationError};
pub(crate) use communication::{ChannelTransport, Shared};