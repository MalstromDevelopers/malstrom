//! Streams are logical orders of operations. A Stream can be seen as a series of nodes and edges
//! in the computation graph
mod build_context;
mod operator;
mod operator_context;
mod operator_logic;
mod stream_builder;

pub use build_context::BuildContext;
pub(crate) use build_context::WorkerBuildContext;
pub use operator::Operator;
pub use operator_context::OperatorContext;
pub use operator_logic::{DirectLogic, LogicBuilder};
pub(crate) use operator_logic::{Logic, SafeLogic, SafeLogicWrapper};
pub use stream_builder::{InitialStreamBuilder, Malstrom, StreamBuilder};

use crate::{
    channels::operator_io::{Input, Output},
    types::Kvt,
};
