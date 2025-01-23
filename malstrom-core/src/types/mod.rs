//! Types and traits used accross JetStream
mod data;
mod key;
mod message;
mod operator_partitioner;
mod time;

pub use data::{Data, MaybeData, NoData};
pub use key::{Key, MaybeKey, NoKey};
pub use message::{DataMessage, Message, RescaleChange, RescaleMessage, SuspendMarker};
pub use operator_partitioner::{OperatorId, OperatorPartitioner};
pub use time::{MaybeTime, NoTime, Timestamp};

/// Uniquely identifies a worker in a JetStream cluster
pub type WorkerId = u64;
