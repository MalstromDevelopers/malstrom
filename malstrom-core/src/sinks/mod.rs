//! Sinks for writing data from a Malstrom job
mod stateful;
mod stateless;
mod stdout;
pub use stateful::{StatefulSink, StatefulSinkImpl, StatefulSinkPartition};
pub use stateless::{StatelessSink, StatelessSinkImpl};
pub use stdout::StdOutSink;
