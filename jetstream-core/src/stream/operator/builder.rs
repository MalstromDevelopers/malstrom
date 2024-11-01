//! A builder to build JetStream operators

use crate::{
    channels::selective_broadcast::{full_broadcast, Receiver, Sender}, keyed::distributed::{Acquire, Collect, Interrogate}, snapshot::Barrier, types::{Data, DataMessage, MaybeData, MaybeKey, MaybeTime, Message, OperatorPartitioner, RescaleMessage, SuspendMarker}
};

use super::{
    standard::StandardOperator, AppendableOperator, BuildContext, BuildableOperator,
    OperatorContext, RunnableOperator,
};

type LogicBuilder<KI, VI, TI, KO, VO, TO> =
    dyn FnOnce(&mut BuildContext) -> Box<dyn Logic<KI, VI, TI, KO, VO, TO>>;

/// A builder type to build generic operators
pub struct OperatorBuilder<KI, VI, TI, KO, VO, TO> {
    input: Receiver<KI, VI, TI>,
    // TODO: get rid of the dynamic dispatch here
    logic_builder: Box<LogicBuilder<KI, VI, TI, KO, VO, TO>>,
    output: Sender<KO, VO, TO>,
    label: Option<String>,
}

pub trait Logic<KI, VI, TI, KO, VO, TO>:
    FnMut(&mut Receiver<KI, VI, TI>, &mut Sender<KO, VO, TO>, &mut OperatorContext) + 'static
{
}
impl<
        KI,
        VI,
        KO,
        VO,
        TI,
        TO,
        X: FnMut(&mut Receiver<KI, VI, TI>, &mut Sender<KO, VO, TO>, &mut OperatorContext) + 'static,
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
    pub fn direct<M: Logic<KI, VI, TI, KO, VO, TO>>(logic: M) -> Self {
        Self::built_by(|_| Box::new(logic))
    }

    pub fn built_by<M: Logic<KI, VI, TI, KO, VO, TO>>(
        logic_builder: impl FnOnce(&mut BuildContext) -> M + 'static,
    ) -> Self {
        let input = Receiver::new_unlinked();
        let output = Sender::new_unlinked(full_broadcast);
        Self {
            input,
            logic_builder: Box::new(|ctx| Box::new(logic_builder(ctx))),
            output,
            label: None,
        }
    }

    pub fn new_with_output_partitioning<M: Logic<KI, VI, TI, KO, VO, TO>>(
        logic_builder: impl FnOnce(&BuildContext) -> M + 'static,
        partitioner: impl OperatorPartitioner<KO, VO, TO>,
    ) -> Self {
        let input = Receiver::new_unlinked();
        let output = Sender::new_unlinked(partitioner);
        Self {
            input,
            logic_builder: Box::new(|ctx| Box::new(logic_builder(ctx))),
            output,
            label: None,
        }
    }

    pub fn get_input_mut(&mut self) -> &mut Receiver<KI, VI, TI> {
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
    fn get_output_mut(&mut self) -> &mut Sender<KO, VO, TO> {
        &mut self.output
    }

    fn into_buildable(self: Box<Self>) -> Box<dyn BuildableOperator> {
        self
    }

    fn label(&mut self, label: String) {
        self.label = Some(label)
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
        RunnableOperator::new(operator, self.label, context)
    }

    fn get_label(&self) -> Option<String> {
        self.label.clone()
    }
}
