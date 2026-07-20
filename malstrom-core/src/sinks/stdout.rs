use std::fmt::Debug;

use crate::types::{DataMessage, Kvt};

use super::StatelessSinkImpl;

/// Sink which prints all records to StdOut. This is only meant for testing and debugging.
pub struct StdOutSink;

impl<M> StatelessSinkImpl<M> for StdOutSink
where
    M: Kvt,
    M::Key: Debug,
    M::Value: Debug,
    M::Timestamp: Debug,
{
    fn sink(&mut self, msg: DataMessage<M>) {
        println!(
            "{{ key: {:?}, value: {:?}, timestamp: {:?} }}",
            msg.key, msg.value, msg.timestamp
        )
    }
}
