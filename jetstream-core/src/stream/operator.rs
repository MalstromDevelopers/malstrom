use postbox::Postbox;

use crate::channels::selective_broadcast::{full_broadcast, Receiver, Sender};
/// Operators:
/// Lifecycle:
/// OperatorBuilder -> Appendable Operator -> FrontieredOperator
/// transforms it into the immutable FrontieredOperator
/// which can be used at runtime
use crate::snapshot::PersistenceBackend;
use crate::time::MaybeTime;
use crate::{MaybeKey, OperatorId, OperatorPartitioner, WorkerId};

use crate::Data;

/// This is a type injected to logic function at runtime
/// and cotains context, whicht the logic generally can not change
/// but utilize
pub struct OperatorContext<'a> {
    pub worker_id: WorkerId,
    pub operator_id: OperatorId,
    pub communication: &'a Postbox<WorkerId>,
}

pub struct BuildContext<P> {
    pub worker_id: WorkerId,
    pub operator_id: OperatorId,
    persistence_backend: P,
    pub communication: Postbox<WorkerId>,
}
impl<P> BuildContext<P>
where
    P: PersistenceBackend,
{
    pub(crate) fn new(
        worker_id: WorkerId,
        operator_id: OperatorId,
        persistence_backend: P,
        communication: Postbox<WorkerId>,
    ) -> Self {
        Self {
            worker_id,
            operator_id,
            persistence_backend,
            communication,
        }
    }

    pub fn load_state<S>(&self) -> Option<S> {
        self.persistence_backend.load(self.operator_id)
    }
}

// AppendableOperator -> BuildableOperator -> RunnableOperator

/// An Operator which can have output added and can be turned
/// into a FrontieredOperator
/// This trait exists mainly for type erasure, so that the Jetstream
/// need not know the input type of its last operator
pub trait AppendableOperator<K, V, T, P> {
    fn get_output_mut(&mut self) -> &mut Sender<K, V, T, P>;

    fn into_buildable(self: Box<Self>) -> Box<dyn BuildableOperator<P>>;
}

/// An operator which can be turned into a runnable operator, by supplying a BuildContext
pub trait BuildableOperator<P> {
    fn into_runnable(self: Box<Self>, context: BuildContext<P>) -> RunnableOperator;
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
}

/// An Operator which does nothing except passing data along
pub fn pass_through_operator<K: MaybeKey, V: Data, T: MaybeTime, P>(
) -> OperatorBuilder<K, V, T, K, V, T, P> {
    OperatorBuilder::direct(|input, output, _ctx| {
        if let Some(x) = input.recv() {
            output.send(x)
        }
    })
}

/// A builder type to build generic operators
pub struct OperatorBuilder<KI, VI, TI, KO, VO, TO, P> {
    input: Receiver<KI, VI, TI, P>,
    // TODO: get rid of the dynamic dispatch here
    logic_builder: Box<dyn FnOnce(&BuildContext<P>) -> Box<dyn Logic<KI, VI, TI, KO, VO, TO, P>>>,
    output: Sender<KO, VO, TO, P>,
}

pub trait Logic<KI, VI, TI, KO, VO, TO, P>:
    FnMut(&mut Receiver<KI, VI, TI, P>, &mut Sender<KO, VO, TO, P>, &mut OperatorContext) + 'static
{
}
impl<
        KI,
        VI,
        KO,
        VO,
        TI,
        TO,
        P,
        X: FnMut(&mut Receiver<KI, VI, TI, P>, &mut Sender<KO, VO, TO, P>, &mut OperatorContext)
            + 'static,
    > Logic<KI, VI, TI, KO, VO, TO, P> for X
{
}

impl<KI, VI, TI, KO, VO, TO, P> OperatorBuilder<KI, VI, TI, KO, VO, TO, P>
where
    KI: MaybeKey,
    VI: Data,
    KO: MaybeKey,
    VO: Data,
    TI: MaybeTime,
    TO: MaybeTime,
    // P: PersistenceBackend,
{
    pub fn direct<M: Logic<KI, VI, TI, KO, VO, TO, P>>(logic: M) -> Self {
        Self::built_by(|_| Box::new(logic))
    }

    pub fn built_by<M: Logic<KI, VI, TI, KO, VO, TO, P>>(
        logic_builder: impl FnOnce(&BuildContext<P>) -> M + 'static,
    ) -> Self {
        let input = Receiver::new_unlinked();
        let output = Sender::new_unlinked(full_broadcast);
        Self {
            input,
            logic_builder: Box::new(|ctx| Box::new(logic_builder(ctx))),
            output,
        }
    }

    pub fn new_with_output_partitioning<M: Logic<KI, VI, TI, KO, VO, TO, P>>(
        logic_builder: impl FnOnce(&BuildContext<P>) -> M + 'static,
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

    pub fn get_input_mut(&mut self) -> &mut Receiver<KI, VI, TI, P> {
        &mut self.input
    }
}

impl<KI, VI, TI, KO, VO, TO, P> AppendableOperator<KO, VO, TO, P>
    for OperatorBuilder<KI, VI, TI, KO, VO, TO, P>
where
    KI: MaybeKey,
    VI: Data,
    TI: MaybeTime,
    KO: MaybeKey,
    VO: Data,
    TO: MaybeTime,
    P: 'static,
{
    fn get_output_mut(&mut self) -> &mut Sender<KO, VO, TO, P> {
        &mut self.output
    }

    fn into_buildable(self: Box<Self>) -> Box<dyn BuildableOperator<P>> {
        self
    }
}

impl<KI, VI, TI, KO, VO, TO, P> BuildableOperator<P> for OperatorBuilder<KI, VI, TI, KO, VO, TO, P>
where
    KI: MaybeKey,
    VI: Data,
    TI: MaybeTime,
    KO: MaybeKey,
    VO: Data,
    TO: MaybeTime,
    P: 'static,
{
    fn into_runnable(self: Box<Self>, context: BuildContext<P>) -> RunnableOperator {
        let operator = StandardOperator {
            input: self.input,
            logic: (self.logic_builder)(&context),
            output: self.output,
        };
        RunnableOperator::new(operator, context)
    }
}

pub struct StandardOperator<KI, VI, TI, KO, VO, TO, P> {
    input: Receiver<KI, VI, TI, P>,
    // TODO: get rid of the dynamic dispatch here
    logic: Box<dyn Logic<KI, VI, TI, KO, VO, TO, P>>,
    output: Sender<KO, VO, TO, P>,
}

impl<KI, VI, TI, KO, VO, TO, P> Operator for StandardOperator<KI, VI, TI, KO, VO, TO, P> {
    fn step(&mut self, context: &mut OperatorContext) {
        (self.logic)(&mut self.input, &mut self.output, context);
    }

    fn has_queued_work(&self) -> bool {
        !self.input.is_empty()
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
//         operator: OperatorBuilder<KI, VI, TI, KO, VO, TO, P>,
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
    communication: Postbox<WorkerId>,
    operator: Box<dyn Operator>,
}

impl RunnableOperator {
    pub fn new<P>(operator: impl Operator + 'static, context: BuildContext<P>) -> Self {
        RunnableOperator {
            worker_id: context.worker_id,
            operator_id: context.operator_id,
            communication: context.communication,
            operator: Box::new(operator),
        }
    }

    pub fn step(&mut self) {
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
