//! Streams are logical orders of operations. A Stream can be seen as a series of nodes and edges
//! in the computation graph
mod builder;
mod operator;

pub use builder::StreamBuilder;
pub use operator::{BuildContext, Logic, LogicWrapper, OperatorBuilder, OperatorContext};

pub(super) use operator::{AppendableOperator, BuildableOperator, RunnableOperator};
