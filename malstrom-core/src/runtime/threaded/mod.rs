//! A [malstrom::runtime::RuntimeFlavor](RuntimeFlavors) using OS threads to provision workers
mod communication;
mod multi;
mod single;

pub use communication::InterThreadCommunication;
pub(crate) use communication::Shared;
pub use multi::MultiThreadRuntime;
pub use single::SingleThreadRuntime;
pub use single::SingleThreadRuntimeFlavor;
