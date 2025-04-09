use std::fmt::Debug;

use super::StatelessSinkImpl;

/// Sink which prints all records to StdOut. This is only meant for testing and debugging.
pub struct StdOutSink;

impl<K, V, T> StatelessSinkImpl<K, V, T> for StdOutSink
where
    K: Debug,
    V: Debug,
    T: Debug,
{
    fn sink(&mut self, msg: crate::types::DataMessage<K, V, T>) {
        println!(
            "{{ key: {:?}, value: {:?}, timestamp: {:?} }}",
            msg.key, msg.value, msg.timestamp
        )
    }
}
