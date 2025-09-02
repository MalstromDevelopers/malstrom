//! Malstrom is a framework for building distributed, stateful stream processes.
pub mod channels;
pub mod coordinator;
pub mod errorhandling;
pub mod keyed;
pub mod operators;
pub mod runtime;
pub mod sinks;
pub mod snapshot;
pub mod sources;
pub mod stream;
pub mod types;
pub mod worker;

#[cfg(test)]
pub(crate) mod testing;
