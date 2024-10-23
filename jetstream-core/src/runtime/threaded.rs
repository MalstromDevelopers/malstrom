mod communication;
mod multi;
mod single;

pub(crate) use communication::Shared;
pub use communication::{InterThreadCommunication, InterThreadCommunicationError};
pub use multi::MultiThreadRuntime;
pub use single::SingleThreadRuntime;
pub use single::SingleThreadRuntimeFlavor;
