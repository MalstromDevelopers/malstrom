//! A builder to build JetStream operators

use std::{
    hash::{Hash, Hasher},
    marker::PhantomData,
};

use crate::{
    channels::operator_io::{Input, Output, full_broadcast},
    stream::{
        GetInput, GetOutput, Logic, OperatorContext,
        operator::{BuildableOperator, RunnableOperator, traits::RunOperator},
    },
    types::{Data, Kvt, MaybeKey, MaybeTime, Message},
};

use super::BuildContext;

/// A builder type to build generic operators
pub struct Operator<M: Kvt, B, N: Kvt> {
    pub(crate) input: Input<M>,
    // TODO: get rid of the dynamic dispatch here
    logic_builder: B,
    pub(crate) output: Output<N>,
    operator_id: u64,
    name: String, // human readable name for debugging
}

pub(crate) trait IntoBuildable {
    fn into_buildable(self) -> impl BuildableOperator;
}

impl<M, B, N> IntoBuildable for Operator<M, B, N>
where
    M: Kvt,
    N: Kvt,
    B: LogicBuilder<M, N>,
{
    fn into_buildable(self) -> impl BuildableOperator {
        self
    }
}

impl<M, B, N> BuildableOperator for Operator<M, B, N>
where
    M: Kvt,
    N: Kvt,
    B: LogicBuilder<M, N>,
{
    fn into_runnable(
        self: Box<Self>,
        rt: &tokio::runtime::Handle,
        context: &mut BuildContext,
    ) -> super::RunnableOperator {
        let logic = rt.block_on(self.logic_builder.build(context));
        let operator = BuiltOperator {
            input: self.input,
            logic,
            output: self.output,
        };
        RunnableOperator::new(operator, context)
    }

    fn get_name(&self) -> &str {
        &self.name
    }

    fn get_id(&self) -> u64 {
        hash_op_name(&self.name)
    }
}

struct BuiltOperator<M: Kvt, L, N: Kvt> {
    input: Input<M>,
    logic: L,
    output: Output<N>,
}

impl<M, L, N> RunOperator for BuiltOperator<M, L, N>
where
    M: Kvt,
    L: Logic<M, N>,
    N: Kvt,
{
    fn schedule(&mut self, ctx: &mut OperatorContext, rt: &tokio::runtime::LocalRuntime) {
        rt.block_on(self.logic.apply(&mut self.input, &mut self.output, ctx))
    }

    fn has_queued_work(&self) -> bool {
        self.input.can_progress()
    }

    fn is_finalized(&self) -> bool {
        <N as Kvt>::Timestamp::CHECK_FINISHED(self.output.get_frontier())
            && <M as Kvt>::Timestamp::CHECK_FINISHED(self.input.get_frontier())
            && !self.input.can_progress()
    }

    fn is_suspended(&self) -> bool {
        self.output.is_suspended()
    }
}
/// A schedulable logic, usually a function, which will repeatedly be called by the worker
/// to progress the Malstrom job.
/// Usually it does not make sense to implement this trait directly. Consider using
/// [malstrom::operators::StatefulLogic](StatefulLogic) instead.
// pub trait Logic<KI, VI, TI, KO, VO, TO>:
//     FnMut(&mut Input<KI, VI, TI>, &mut Output<KO, VO, TO>, &mut OperatorContext) + 'static
// {
// }
// impl<
//         KI,
//         VI,
//         KO,
//         VO,
//         TI,
//         TO,
//         X: FnMut(&mut Input<KI, VI, TI>, &mut Output<KO, VO, TO>, &mut OperatorContext) + 'static,
//     > Logic<KI, VI, TI, KO, VO, TO> for X
// {
// }

pub trait LogicBuilder<M: Kvt, N: Kvt>: 'static {
    type Logic: Logic<M, N>;
    async fn build(self, ctx: &mut BuildContext) -> Self::Logic;
}

pub struct DirectLogic<L> {
    logic: L,
}

impl<M, N, L> LogicBuilder<M, N> for DirectLogic<L>
where
    M: Kvt,
    N: Kvt,
    L: Logic<M, N> + 'static,
{
    type Logic = L;
    async fn build(self, _ctx: &mut BuildContext<'_>) -> Self::Logic {
        self.logic
    }
}

impl<M, N, F, Fut> LogicBuilder<M, N> for F
where
    F: FnOnce(&mut BuildContext) -> Fut + 'static,
    Fut: Future,
    Fut::Output: Logic<M, N>,
    M: Kvt,
    N: Kvt,
{
    type Logic = <Fut as Future>::Output;
    async fn build(self, ctx: &mut BuildContext<'_>) -> Self::Logic {
        (self)(ctx).await
    }
}

impl<M, L, N> Operator<M, DirectLogic<L>, N>
where
    M: Kvt,
    N: Kvt,
    L: Logic<M, N>,
{
    /// Create a new stream operator directly by supplying a name and a function which will
    /// repeatedly be called (scheduled) by the worker
    pub fn direct(name: String, logic: L) -> Self {
        Self::built_by(name, DirectLogic { logic })
    }
}

impl<M, B, N> Operator<M, B, N>
where
    M: Kvt,
    B: LogicBuilder<M, N>,
    N: Kvt,
{
    /// Create a new stream operator from the given name and a function which will return the
    /// actually scheduled function at build time. This is useful to utilize information from the
    /// [BuildContext]. If information from the [BuildContext] is not needed, consider calling
    /// [Self::direct] instead.
    pub fn built_by(name: String, logic_builder: B) -> Self {
        let input = Input::new_unlinked();
        let output = Output::new_unlinked(full_broadcast);
        Self {
            input,
            logic_builder,
            output,
            operator_id: hash_op_name(&name),
            name: name,
        }
    }

    pub(crate) fn new_with_output(name: String, logic_builder: B, output: Output<N>) -> Self {
        let input = Input::new_unlinked();
        Self {
            input,
            logic_builder: logic_builder,
            output,
            operator_id: hash_op_name(&name),
            name: name.to_owned(),
        }
    }
}

impl<M, B, N> GetOutput<N> for Operator<M, B, N>
where
    N: Kvt,
    M: Kvt,
{
    fn get_output_mut(&mut self) -> &mut Output<N> {
        &mut self.output
    }
}

impl<M, B, N> GetInput<M> for Operator<M, B, N>
where
    N: Kvt,
    M: Kvt,
{
    fn get_input_mut(&mut self) -> &mut Input<M> {
        &mut self.input
    }
}

// impl<KI, VI, TI, KO, VO, TO> AppendableOperator<KO, VO, TO>
//     for OperatorBuilder<KI, VI, TI, KO, VO, TO>
// where
//     KI: MaybeKey,
//     VI: Data,
//     TI: MaybeTime,
//     KO: MaybeKey,
//     VO: Data,
//     TO: MaybeTime,
// {
//     fn get_output_mut(&mut self) -> &mut Output<KO, VO, TO> {
//         &mut self.output
//     }

//     fn into_buildable(self: Box<Self>) -> Box<dyn BuildableOperator> {
//         self
//     }
// }

// impl<KI, VI, TI, KO, VO, TO> BuildableOperator for OperatorBuilder<KI, VI, TI, KO, VO, TO>
// where
//     KI: MaybeKey,
//     VI: Data,
//     TI: MaybeTime,
//     KO: MaybeKey,
//     VO: Data,
//     TO: MaybeTime,
// {
//     fn into_runnable(self: Box<Self>, context: &mut BuildContext) -> RunnableOperator {
//         let operator = StandardOperator {
//             input: self.input,
//             logic: (self.logic_builder)(context),
//             output: self.output,
//         };
//         RunnableOperator::new(operator, context)
//     }

//     fn get_name(&self) -> &str {
//         &self.name
//     }

//     fn get_id(&self) -> u64 {
//         self.operator_id
//     }
// }

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
