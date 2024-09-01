//! The standard do-it-all baseline operator used for everything.

use crate::{channels::selective_broadcast::{Receiver, Sender}, types::MaybeTime};

use super::{traits::Operator, Logic, OperatorContext};

/// Operator wrapped by RunnableOperator
pub(super) struct StandardOperator<KI, VI, TI, KO, VO, TO> {
    pub(super) input: Receiver<KI, VI, TI>,
    // TODO: get rid of the dynamic dispatch here
    pub(super) logic: Box<dyn Logic<KI, VI, TI, KO, VO, TO>>,
    pub(super) output: Sender<KO, VO, TO>,
}


impl<KI, VI, TI, KO, VO, TO> Operator for StandardOperator<KI, VI, TI, KO, VO, TO> where KO: Clone, VO: Clone, TO: MaybeTime {
    
    fn is_finished(&self) -> bool {
        TO::CHECK_FINISHED(self.output.get_frontier()) && self.input.is_empty()
    }
    
    fn step(&mut self, context: &mut OperatorContext) {
        (self.logic)(&mut self.input, &mut self.output, context);
    }
    
    fn has_queued_work(&self) -> bool {
        !self.input.is_empty()
    }
}
