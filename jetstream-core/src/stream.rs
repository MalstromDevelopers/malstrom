mod builder;
mod operator;

pub use operator::{OperatorContext, OperatorBuilder, BuildContext, Logic};
pub use builder::JetStreamBuilder;

pub(super) use operator::{AppendableOperator, RunnableOperator, pass_through_operator, BuildableOperator};