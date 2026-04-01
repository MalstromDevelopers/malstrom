//! A [malstrom::runtime::RuntimeFlavor](RuntimeFlavors) using OS threads to provision workers
mod communication;
mod single;

pub use single::SingleThreadRuntime;
pub use single::SingleThreadRuntimeFlavor;

// TODO
// mod multi;
// pub use multi::MultiThreadRuntime;