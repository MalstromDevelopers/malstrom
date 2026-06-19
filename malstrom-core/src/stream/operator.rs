//! A builder to build JetStream operators

use std::{
    hash::{Hash, Hasher},
    marker::PhantomData,
    rc::Rc,
};

use tokio::runtime::LocalRuntime;

use crate::{
    channels::operator_io::{Input, Output, full_broadcast},
    stream::{DirectLogic, Logic, LogicBuilder, OperatorContext, WorkerBuildContext},
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

        let mut build_ctx = build_ctx
            .await
            .to_build_context(self.operator_id, self.name);
        let mut logic = self.logic_builder.build(&mut build_ctx).await;
        let mut operator_context = OperatorContext::new(build_ctx.worker_id, self.operator_id);

        let mut output_closed = self.output.get_closed_signal();

        loop {
            tokio::select! {
                _ = logic.apply(&mut self.input, &mut self.output, &mut operator_context) => (),
                    // can not possibly process more messages
                _ = output_closed.wait_for() => return
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

impl<M, L, N> Operator<M, DirectLogic<L>, N>
where
    M: Kvt,
    N: Kvt,
    L: Logic<M, N>,
{
    /// Create a new stream operator directly by supplying a name and a function which will
    /// repeatedly be called (scheduled) by the worker
    pub fn direct(name: String, logic: L) -> Self {
        Self::built_by(name, DirectLogic::new(logic))
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

    pub(crate) fn get_output_mut(&mut self) -> &mut Output<N> {
        &mut self.output
    }

    pub(crate) fn get_input_mut(&mut self) -> &mut Input<M> {
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
