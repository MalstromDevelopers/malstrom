use std::any::Any;
use std::ops::Range;

use postbox::{Client, Postbox};
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
    communication: &'a mut Postbox<(WorkerId, OperatorId)>,
}

impl<'a> OperatorContext<'a> {
    /// Create a client for inter-worker communication
    pub fn create_communication_client<T: postbox::Data>(&mut self, other_worker: WorkerId, other_operator: OperatorId) -> Client<T> {
        self.communication.new_client((other_worker, other_operator), (self.worker_id, self.operator_id)).unwrap()
    }
}

pub struct BuildContext<'a> {
    pub worker_id: WorkerId,
    pub operator_id: OperatorId,
    pub label: String,
    persistence_backend: Box<dyn PersistenceBackend>,
    communication: &'a mut Postbox<(WorkerId, OperatorId)>,
    worker_ids: Range<usize>
}
impl<'a> BuildContext<'a> {
    pub(crate) fn new(
        worker_id: WorkerId,
        operator_id: OperatorId,
        label: String,
        persistence_backend: Box<dyn PersistenceBackend>,
        communication: &'a mut Postbox<(WorkerId, OperatorId)>,
        worker_ids: Range<usize>
    ) -> Self {
        Self {
            worker_id,
            operator_id,
            label,
            persistence_backend,
            communication,
            worker_ids
        }
    }

    pub fn load_state<S: Serialize + DeserializeOwned>(&self) -> Option<S> {
        self.persistence_backend
            .load(&self.operator_id)
            .map(deserialize_state)
    }

    /// Get the IDs of all workers (including this one) which are part of the cluster
    /// at build time.
    /// NOTE: JetStream is designed to scale dynamically, so this information may become outdated
    /// at runtime
    pub fn get_worker_ids(&self) -> Range<WorkerId> {
        self.worker_ids.clone()
    }

    /// Create a client for inter-worker communication
    pub fn create_communication_client<T: postbox::Data>(&mut self, other_worker: WorkerId, other_operator: OperatorId) -> Client<T> {
        self.communication.new_client((other_worker, other_operator), (self.worker_id, self.operator_id)).unwrap()
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

    /// Add a label to this operator which will show up in traces
    fn label(&mut self, label: String) -> ();
}

/// An operator which can be turned into a runnable operator, by supplying a BuildContext
pub trait BuildableOperator {
    fn into_runnable(self: Box<Self>, context: &mut BuildContext) -> RunnableOperator;
    fn get_label(&self) -> Option<String>;
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
    logic_builder: Box<dyn FnOnce(&mut BuildContext) -> Box<dyn Logic<KI, VI, TI, KO, VO, TO>>>,
    output: Sender<KO, VO, TO>,
    label: Option<String>
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
        logic_builder: impl FnOnce(&mut BuildContext) -> M + 'static,
    ) -> Self {
        let input = Receiver::new_unlinked();
        let output = Sender::new_unlinked(full_broadcast);
        Self {
            input,
            logic_builder: Box::new(|mut ctx| Box::new(logic_builder(&mut ctx))),
            output,
            label: None
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
    
    fn label(&mut self, label: String) -> () {
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
    operator: Box<dyn Operator>,
    label: String,
}


impl  RunnableOperator {
    pub fn new(operator: impl Operator + 'static, label: Option<String>, context: &mut BuildContext) -> Self {
        RunnableOperator {
            worker_id: context.worker_id,
            operator_id: context.operator_id,
            operator: Box::new(operator),
            label: label.unwrap_or("NO_LABEL".into()),
        }
    }

    pub fn step(&mut self, communication: &mut Postbox<(WorkerId, OperatorId)>,) {
        let span = tracing::info_span!("scheduling::run_operator", operator_id = self.operator_id, label = self.label);
        let _span_guard = span.enter();
        let mut context = OperatorContext {
            worker_id: self.worker_id,
            operator_id: self.operator_id,
            communication
        };

        self.operator.step(&mut context)
    }
    pub fn has_queued_work(&self) -> bool {
        self.operator.has_queued_work()
    }
}
