//! Trait for inter-operator routing of messages

use super::DataMessage;

/// Uniquely identifies an operator within a worker
pub type OperatorId = u64;

/// Marker trait for functions which determine inter-operator routing
/// The OperatorPartitioner is a function which receives as arguments:
/// - a reference to every message to be partitioned
/// - the count of available receivers
///
/// And should emit the **indices** of the receivers, which should receive this message
pub trait OperatorPartitioner<K, V, T>:
    Fn(&DataMessage<K, V, T>, &mut [bool]) -> () + 'static
{
}
impl<K, V, T, U> OperatorPartitioner<K, V, T> for U where
    U: Fn(&DataMessage<K, V, T>, &mut [bool]) -> () + 'static
{
}
