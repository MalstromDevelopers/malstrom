//! Streams are logical orders of operations. A Stream can be seen as a series of nodes and edges
//! in the computation graph
mod builder;
mod operator;

pub use builder::{InitialStreamBuilder, Malstrom, StreamBuilder};
pub(crate) use operator::WorkerBuildContext;
pub use operator::{BuildContext, DirectLogic, LogicBuilder, Operator, OperatorContext};
pub(crate) use operator::{Logic, SafeLogic, SafeLogicWrapper};
// pub(super) use operator::{AppendableOperator, BuildableOperator, RunnableOperator};

use crate::{
    channels::operator_io::{Input, Output},
    types::Kvt,
};

trait GetOutput<M: Kvt> {
    fn get_output_mut(&mut self) -> &mut Output<M>;
}

trait GetInput<M: Kvt> {
    fn get_input_mut(&mut self) -> &mut Input<M>;
}
