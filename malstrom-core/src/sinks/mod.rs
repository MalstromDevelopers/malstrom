//! Sinks for writing data from a Malstrom job
mod stateful;
mod stateless;
mod stdout;
mod vec_sink;
pub use stateful::{StatefulSink, StatefulSinkImpl, StatefulSinkPartition};
pub use stateless::{StatelessSink, StatelessSinkImpl};
pub use stdout::StdOutSink;
pub use vec_sink::VecSink;