//! Types and traits used accross JetStream
mod message;
mod key;
mod data;
mod time;
mod operator_partitioner;


pub use message::{DataMessage, Message, RescaleMessage, SuspendMarker};
pub use key::{Key, NoKey, MaybeKey};
pub use data::{Data, NoData, MaybeData};
pub use time::{Timestamp, NoTime, MaybeTime};
pub use operator_partitioner::{OperatorPartitioner, OperatorId};

/// Uniquely identifies a worker in a JetStream cluster
pub type WorkerId = usize;
