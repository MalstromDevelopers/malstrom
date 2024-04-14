use std::any::Any;

use postbox::Postbox;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::channels::selective_broadcast::{self, full_broadcast, Receiver, Sender};
/// Operators:
/// Lifecycle:
/// OperatorBuilder -> Appendable Operator -> FrontieredOperator
/// transforms it into the immutable FrontieredOperator
/// which can be used at runtime
use crate::snapshot::{deserialize_state, PersistenceBackend};
use crate::time::MaybeTime;
use crate::{MaybeKey, OperatorId, OperatorPartitioner, WorkerId};

use crate::Data;

/// This is a type injected to logic function at runtime
/// and cotains context, whicht the logic generally can not change
/// but utilize
pub struct OperatorContext<'a> {
    pub worker_id: WorkerId,
    pub operator_id: OperatorId,
    pub communication: &'a Postbox<WorkerId, OperatorId>,
}

impl<'a> OperatorContext<'a> {
    /// Instruct the runtime to drop this operator immediately.
    /// When an operator has invoked this method, it will never be
    /// scheduled again.
    pub fn drop_this_operator(&self) -> () {
        todo!()
    }

    // /// Instruct the worker to create n more instances of this operator
    // /// This will be created as siblings of this operator, sharing the same
    // /// position in the operator graph. They will be connected to the same
    // /// upstream and downstream nodes.
    // pub fn duplicate_this_operator(&self, n: usize) -> Result<(), Box<dyn std::error::Error>> {
    //     todo!()
    // }
}

pub struct BuildContext {
    pub worker_id: WorkerId,
    pub operator_id: OperatorId,
    persistence_backend: Box<dyn PersistenceBackend>,
    pub communication: Postbox<WorkerId, OperatorId>,
}
impl BuildContext {
    pub(crate) fn new(
        worker_id: WorkerId,
        operator_id: OperatorId,
        persistence_backend: Box<dyn PersistenceBackend>,
        communication: Postbox<WorkerId, OperatorId>,
    ) -> Self {
        Self {
            worker_id,
            operator_id,
            persistence_backend,
            communication,
        }
    }

    pub fn load_state<S: Serialize + DeserializeOwned>(&self) -> Option<S> {
        self.persistence_backend
            .load(&self.operator_id)
            .map(deserialize_state)
    }
}

// AppendableOperator -> BuildableOperator -> RunnableOperator

/// An Operator which can have output added and can be turned
/// into a FrontieredOperator
/// This trait exists mainly for type erasure, so that the Jetstream
/// need not know the input type of its last operator
pub trait AppendableOperator<K, V, T> {
    fn get_output_mut(&mut self) -> &mut Sender<K, V, T>;

    fn into_buildable(self: Box<Self>) -> Box<dyn BuildableOperator>;
}

/// An operator which can be turned into a runnable operator, by supplying a BuildContext
pub trait BuildableOperator {
    fn into_runnable(self: Box<Self>, context: BuildContext) -> RunnableOperator;
}

/// Each runnable operator contains an object of this trait which is the actual logic that will get executed
pub trait Operator {
    /// Calling step instructs the operator, that it should attempt to make
    /// progress. There is absolutely no assumption on what "progress" means,
    /// but it is implied, that the operator reads its input and writes
    /// to its output
    fn step(&mut self, context: &mut OperatorContext);

    /// still not happy with this function name
    fn has_queued_work(&self) -> bool;

    // /// create a new instance of this operator
    // /// NOTE: The return type is only constrained to be
    // /// of trait "Operator", but it is expected, that this
    // /// method returns the exact same type as self
    // fn duplicate(&self) -> Box<dyn Fn(&BuildContext) -> dyn Operator>;
}

/// An Operator which does nothing except passing data along
pub fn pass_through_operator<K: MaybeKey, V: Data, T: MaybeTime>(
) -> OperatorBuilder<K, V, T, K, V, T> {
    OperatorBuilder::direct(|input, output, _ctx| {
        if let Some(x) = input.recv() {
            output.send(x)
        }
    })
}

/// A builder type to build generic operators
pub struct OperatorBuilder<KI, VI, TI, KO, VO, TO> {
    input: Receiver<KI, VI, TI>,
    // TODO: get rid of the dynamic dispatch here
    logic_builder: Box<dyn FnOnce(&BuildContext) -> Box<dyn Logic<KI, VI, TI, KO, VO, TO>>>,
    output: Sender<KO, VO, TO>,
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
    // P: PersistenceBackend,
{
    pub fn direct<M: Logic<KI, VI, TI, KO, VO, TO>>(logic: M) -> Self {
        Self::built_by(|_| Box::new(logic))
    }

    pub fn built_by<M: Logic<KI, VI, TI, KO, VO, TO>>(
        logic_builder: impl FnOnce(&BuildContext) -> M + 'static,
    ) -> Self {
        let input = Receiver::new_unlinked();
        let output = Sender::new_unlinked(full_broadcast);
        Self {
            input,
            logic_builder: Box::new(|ctx| Box::new(logic_builder(ctx))),
            output,
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
    fn into_runnable(self: Box<Self>, context: BuildContext) -> RunnableOperator {
        let operator = StandardOperator {
            input: self.input,
            logic: (self.logic_builder)(&context),
            output: self.output,
        };
        RunnableOperator::new(operator, context)
    }
}

pub struct StandardOperator<KI, VI, TI, KO, VO, TO> {
    input: Receiver<KI, VI, TI>,
    // TODO: get rid of the dynamic dispatch here
    logic: Box<dyn Logic<KI, VI, TI, KO, VO, TO>>,
    output: Sender<KO, VO, TO>,
}

impl<KI, VI, TI, KO, VO, TO> Operator for StandardOperator<KI, VI, TI, KO, VO, TO> {
    fn step(&mut self, context: &mut OperatorContext) {
        (self.logic)(&mut self.input, &mut self.output, context);
    }

    fn has_queued_work(&self) -> bool {
        !self.input.is_empty()
    }
}

impl<KI, VI, TI, KO, VO, TO> StandardOperator<KI, VI, TI, KO, VO, TO>
where
    KI: MaybeKey,
    VI: Data,
    TI: MaybeTime,
    KO: MaybeKey,
    VO: Data,
    TO: MaybeTime,
{
    fn add_input(&mut self, maybe_sender: &mut dyn Any) -> () {
        let sender = maybe_sender.downcast_mut().unwrap();
        selective_broadcast::link(sender, &mut self.input)
    }
}

// pub struct FrontieredOperator {
//     operator: Box<dyn Operator>,
// }

// impl FrontieredOperator {
//     fn new<
//         KI: Key,
//         VI: Data,
//         TI: Timestamp,
//         KO: Key,
//         VO: Data,
//         TO: Timestamp,
//         P: PersistenceBackend,
//     >(
//         operator: OperatorBuilder<KI, VI, TI, KO, VO, TO>,
//     ) -> Self {
//         Self {
//             operator: Box::new(operator),
//         }
//     }

//     pub fn build(
//         self,
//         worker_id: WorkerId,
//         operator_id: OperatorId,
//         communication: Postbox<WorkerId>,
//     ) -> RunnableOperator {
//         RunnableOperator {
//             worker_id,
//             operator_id,
//             communication,
//             operator: self.operator,
//         }
//     }
// }

pub struct RunnableOperator {
    worker_id: WorkerId,
    operator_id: OperatorId,
    communication: Postbox<WorkerId, OperatorId>,
    operator: Box<dyn Operator>,
}

pub(crate) struct StepResponse {
    // true if this operator should be kept in the operator graph
    keep: bool,
    // Number of duplicates of this operator to create
    duplicate: usize,
}

impl RunnableOperator {
    pub fn new(operator: impl Operator + 'static, context: BuildContext) -> Self {
        RunnableOperator {
            worker_id: context.worker_id,
            operator_id: context.operator_id,
            communication: context.communication,
            operator: Box::new(operator),
        }
    }

    pub fn step(&mut self) {
        let span = tracing::info_span!("Running Operator ", %self.operator_id);
        let _span_guard = span.enter();
        let mut context = OperatorContext {
            worker_id: self.worker_id,
            operator_id: self.operator_id,
            communication: &self.communication,
        };

        self.operator.step(&mut context)
    }
    pub fn has_queued_work(&self) -> bool {
        self.operator.has_queued_work()
    }
}
