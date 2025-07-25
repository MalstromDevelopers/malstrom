//! A builder to build JetStream operators

use std::hash::{Hash, Hasher};

use crate::{
    channels::operator_io::{full_broadcast, Input, Output},
    types::{Data, MaybeKey, MaybeTime},
};

use super::{
    standard::StandardOperator, AppendableOperator, BuildContext, BuildableOperator,
    OperatorContext, RunnableOperator,
};

type LogicBuilder<KI, VI, TI, KO, VO, TO> =
    dyn FnOnce(&mut BuildContext) -> Box<dyn Logic<KI, VI, TI, KO, VO, TO>>;

/// A builder type to build generic operators
pub struct OperatorBuilder<KI, VI, TI, KO, VO, TO> {
    input: Input<KI, VI, TI>,
    // TODO: get rid of the dynamic dispatch here
    logic_builder: Box<LogicBuilder<KI, VI, TI, KO, VO, TO>>,
    output: Output<KO, VO, TO>,
    operator_id: u64,
    name: String, // human readable name for debugging
}

/// A schedulable logic, usually a function, which will repeatedly be called by the worker
/// to progress the Malstrom job.
/// Usually it does not make sense to implement this trait directly. Consider using
/// [malstrom::operators::StatefulLogic](StatefulLogic) instead.
pub trait Logic<KI, VI, TI, KO, VO, TO>:
    FnMut(&mut Input<KI, VI, TI>, &mut Output<KO, VO, TO>, &mut OperatorContext) + 'static
{
}
impl<
        KI,
        VI,
        KO,
        VO,
        TI,
        TO,
        X: FnMut(&mut Input<KI, VI, TI>, &mut Output<KO, VO, TO>, &mut OperatorContext) + 'static,
    > Logic<KI, VI, TI, KO, VO, TO> for X
{
}

impl<KI, VI, TI, KO, VO, TO> OperatorBuilder<KI, VI, TI, KO, VO, TO>
where
    KI: MaybeKey,
    VI: Data,
    KO: MaybeKey,
    VO: Data,
    TI: MaybeTime,
    TO: MaybeTime,
{
    /// Create a new stream operator directly by supplying a name and a function which will
    /// repeatedly be called (scheduled) by the worker
    pub fn direct<M: Logic<KI, VI, TI, KO, VO, TO>>(name: &str, logic: M) -> Self {
        Self::built_by(name, |_| Box::new(logic))
    }

    /// Create a new stream operator from the given name and a function which will return the
    /// actually scheduled function at build time. This is useful to utilize information from the
    /// [BuildContext]. If information from the [BuildContext] is not needed, consider calling
    /// [Self::direct] instead.
    pub fn built_by<M: Logic<KI, VI, TI, KO, VO, TO>>(
        name: &str,
        logic_builder: impl FnOnce(&mut BuildContext) -> M + 'static,
    ) -> Self {
        let input = Input::new_unlinked();
        let output = Output::new_unlinked(full_broadcast);
        Self {
            input,
            logic_builder: Box::new(|ctx| Box::new(logic_builder(ctx))),
            output,
            operator_id: hash_op_name(name),
            name: name.to_owned(),
        }
    }

    pub(crate) fn new_with_output<M: Logic<KI, VI, TI, KO, VO, TO>>(
        name: &str,
        logic_builder: impl FnOnce(&BuildContext) -> M + 'static,
        output: Output<KO, VO, TO>,
    ) -> Self {
        let input = Input::new_unlinked();
        Self {
            input,
            logic_builder: Box::new(|ctx| Box::new(logic_builder(ctx))),
            output,
            operator_id: hash_op_name(name),
            name: name.to_owned(),
        }
    }

    pub(crate) fn get_input_mut(&mut self) -> &mut Input<KI, VI, TI> {
        &mut self.input
    }
}

impl<KI, VI, TI, KO, VO, TO> AppendableOperator<KO, VO, TO>
    for OperatorBuilder<KI, VI, TI, KO, VO, TO>
where
    KI: MaybeKey,
    VI: Data,
    TI: MaybeTime,
    KO: MaybeKey,
    VO: Data,
    TO: MaybeTime,
{
    fn get_output_mut(&mut self) -> &mut Output<KO, VO, TO> {
        &mut self.output
    }

    fn into_buildable(self: Box<Self>) -> Box<dyn BuildableOperator> {
        self
    }
}

impl<KI, VI, TI, KO, VO, TO> BuildableOperator for OperatorBuilder<KI, VI, TI, KO, VO, TO>
where
    KI: MaybeKey,
    VI: Data,
    TI: MaybeTime,
    KO: MaybeKey,
    VO: Data,
    TO: MaybeTime,
{
    fn into_runnable(self: Box<Self>, context: &mut BuildContext) -> RunnableOperator {
        let operator = StandardOperator {
            input: self.input,
            logic: (self.logic_builder)(context),
            output: self.output,
        };
        RunnableOperator::new(operator, context)
    }

    fn get_name(&self) -> &str {
        &self.name
    }

    fn get_id(&self) -> u64 {
        self.operator_id
    }
}

fn hash_op_name(name: &str) -> u64 {
    let mut hasher = seahash::SeaHasher::new();
    name.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::hash_op_name;

    /// this test should break if we somehow break hash stability between versions
    /// Breaking hash stability would be bad, as keying of messages would change otherwise.
    /// When you are doing stateful upgrades the state would then be in the wrong place.
    #[test]
    fn hash_is_stable() {
        let h = hash_op_name("The ships hung in the sky in much the same way that bricks don't.");
        assert_eq!(h, 16283470273735909098); // unfortunately it is not 42 :(
    }
}
