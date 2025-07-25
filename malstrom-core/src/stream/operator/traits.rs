//! A series of traits which operators implement
//! These traits essentially exist to perform successive type erasure
//! AppendableOperator -> BuildableOperator -> Operator

use crate::channels::operator_io::Output;

use super::{BuildContext, OperatorContext, RunnableOperator};

/// An Operator which can have output added and can be turned
/// into a FrontieredOperator
/// This trait exists mainly for type erasure, so that the Jetstream
/// need not know the input type of its last operator
pub trait AppendableOperator<K, V, T> {
    fn get_output_mut(&mut self) -> &mut Output<K, V, T>;

    fn into_buildable(self: Box<Self>) -> Box<dyn BuildableOperator>;
}

/// An operator which can be turned into a runnable operator, by supplying a BuildContext
pub trait BuildableOperator {
    fn into_runnable(self: Box<Self>, context: &mut BuildContext) -> RunnableOperator;
    fn get_name(&self) -> &str;
    fn get_id(&self) -> u64;
}

/// Each runnable operator contains an object of this trait which is the actual logic that will get executed
pub trait Operator {
    /// Calling step instructs the operator, that it should attempt to make
    /// progress. There is absolutely no assumption on what "progress" means,
    /// but it is implied, that the operator reads its input and writes
    /// to its output
    fn step(&mut self, context: &mut OperatorContext);

    /// still not happy with this function name
    fn has_queued_work(&self) -> bool;

    /// Indicate to the worker this operator need not run again
    /// The worker will stop execution once all operators are finished
    fn is_finalized(&self) -> bool;

    /// Return true if the operator is suspended and must not be scheduled again before a restart
    fn is_suspended(&self) -> bool;
}
