//! A builder to build JetStream operators

use std::{
    hash::{Hash, Hasher},
    marker::PhantomData,
};

use crate::{
    channels::operator_io::{Input, Output, full_broadcast},
    stream::{GetInput, GetOutput, Logic, OperatorContext, operator::context::WorkerBuildContext},
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

impl<M, B, N> Operator<M, B, N>
where
    M: Kvt,
    N: Kvt,
    B: LogicBuilder<M, N>,
{
    pub(crate) async fn start(mut self, build_ctx: impl Future<Output = WorkerBuildContext>) {
        let name = self.get_name().to_string();
        let (mut build_ctx, completion_ref) = build_ctx
            .await
            .to_build_context(self.operator_id, self.name);
        let mut logic = self.logic_builder.build(&mut build_ctx).await;
        println!("Operator {name} started");
        let mut operator_context = OperatorContext::new(
            build_ctx.worker_id,
            self.operator_id,
            build_ctx.communication,
        );

        let mut completion_ref = Some(completion_ref);
        let is_finalized = || {
            // TODO: Why are we checking both in and output here?
            N::Timestamp::CHECK_FINISHED(self.output.get_frontier())
                && M::Timestamp::CHECK_FINISHED(&self.input.get_frontier())
        };
        
        loop {
            logic
                .apply(&mut self.input, &mut self.output, &mut operator_context)
                .await;
            if N::Timestamp::CHECK_FINISHED(self.output.get_frontier()) {
                // drop completion ref to indicate we won't produce anymore data
                let _ = completion_ref.take();
            }
            if self.output.is_suspended() {
                return;
            }
        }
    }

    pub(crate) fn get_name(&self) -> &str {
        &self.name
    }

    pub(crate) fn get_id(&self) -> u64 {
        hash_op_name(&self.name)
    }
}

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
    async fn build(self, _ctx: &mut BuildContext) -> Self::Logic {
        self.logic
    }
}

impl<M, N, F, L> LogicBuilder<M, N> for F
where
    F: AsyncFnOnce(&mut BuildContext) -> L + 'static,
    L: Logic<M, N>,
    M: Kvt,
    N: Kvt,
{
    type Logic = L;
    async fn build(self, ctx: &mut BuildContext) -> Self::Logic {
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
