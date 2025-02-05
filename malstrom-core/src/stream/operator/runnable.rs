use tracing::debug_span;

use crate::{
    runtime::OperatorOperatorComm,
    types::{OperatorId, WorkerId},
};

use super::{traits::Operator, BuildContext, OperatorContext};

pub struct RunnableOperator {
    worker_id: WorkerId,
    operator_id: OperatorId,
    operator: Box<dyn Operator>,
    name: String,
}

impl RunnableOperator {
    pub fn new(operator: impl Operator + 'static, context: &mut BuildContext) -> Self {
        RunnableOperator {
            worker_id: context.worker_id,
            operator_id: context.operator_id,
            operator: Box::new(operator),
            name: context.operator_name.clone(),
        }
    }

    pub fn step(&mut self, communication: &mut dyn OperatorOperatorComm) {
        let mut context = OperatorContext {
            worker_id: self.worker_id,
            operator_id: self.operator_id,
            communication,
        };
        self.operator.step(&mut context)
    }
    pub fn has_queued_work(&self) -> bool {
        self.operator.has_queued_work()
    }

    /// check if this operator will ever emit a message again
    pub fn is_finalized(&self) -> bool {
        self.operator.is_finalized()
    }

    pub(crate) fn is_suspended(&self) -> bool {
        self.operator.is_suspended()
    }

    pub(crate) fn name(&self) -> &str {
        &self.name
    }
}
