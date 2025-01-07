//! The standard do-it-all baseline operator used for everything.

use crate::{
    channels::operator_io::{Input, Output},
    types::MaybeTime,
};

use super::{traits::Operator, Logic, OperatorContext};

/// Operator wrapped by RunnableOperator
pub(super) struct StandardOperator<KI, VI, TI, KO, VO, TO> {
    pub(super) input: Input<KI, VI, TI>,
    // TODO: get rid of the dynamic dispatch here
    pub(super) logic: Box<dyn Logic<KI, VI, TI, KO, VO, TO>>,
    pub(super) output: Output<KO, VO, TO>,
}

impl<KI, VI, TI, KO, VO, TO> Operator for StandardOperator<KI, VI, TI, KO, VO, TO>
where
    TI: MaybeTime,
    KO: Clone,
    VO: Clone,
    TO: MaybeTime,
{
    fn is_finalized(&self) -> bool {
        TO::CHECK_FINISHED(self.output.get_frontier())
            && TI::CHECK_FINISHED(self.input.get_frontier())
            && !self.input.can_progress()
    }

    fn step(&mut self, context: &mut OperatorContext) {
        (self.logic)(&mut self.input, &mut self.output, context);
    }

    fn has_queued_work(&self) -> bool {
        self.input.can_progress()
    }
}
