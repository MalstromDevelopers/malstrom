//! Channels for exchanging data between stream operators
pub mod operator_io;
pub(crate) mod signal;
pub(crate) mod spsc;
pub(crate) mod alignment;
pub(crate) mod recv_trait;