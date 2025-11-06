use crate::{
    runtime::OperatorOperatorComm,
    stream::operator::traits::RunOperator,
    types::{OperatorId, WorkerId},
};

use super::{BuildContext, OperatorContext};

pub struct RunnableOperator {
    worker_id: WorkerId,
    operator_id: OperatorId,
    operator: Box<dyn RunOperator>,
    name: String,
}

impl RunnableOperator {
    pub fn new(operator: impl RunOperator + 'static, context: &mut BuildContext) -> Self {
        RunnableOperator {
            worker_id: context.worker_id,
            operator_id: context.operator_id,
            operator: Box::new(operator),
            name: context.operator_name.clone(),
        }
    }

    pub fn step(
        &mut self,
        communication: &mut dyn OperatorOperatorComm,
        rt: &tokio::runtime::LocalRuntime,
    ) {
        let mut context = OperatorContext {
            worker_id: self.worker_id,
            operator_id: self.operator_id,
            communication,
        };
        self.operator.schedule(&mut context, rt)
    }

    pub(crate) fn name(&self) -> &str {
        &self.name
    }
}
