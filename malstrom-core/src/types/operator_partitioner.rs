//! Trait for inter-operator routing of messages

use crate::types::Kvt;

use super::DataMessage;

/// Uniquely identifies an operator within a worker
pub type OperatorId = u64;

/// Marker trait for functions which determine inter-operator routing
/// The OperatorPartitioner is a function which receives as arguments:
/// - a reference to every message to be partitioned
/// - the count of available receivers
///
/// And should emit the **indices** of the receivers, which should receive this message
pub trait OperatorPartitioner<M: Kvt>: Fn(&DataMessage<M>, &mut [bool]) + 'static {}
impl<M, U> OperatorPartitioner<M> for U
where
    M: Kvt,
    U: Fn(&DataMessage<M>, &mut [bool]) + 'static,
{
}
