use postbox::Postbox;

use crate::channels::selective_broadcast::{full_broadcast, Receiver, Sender};
/// Operators:
/// Lifecycle:
/// OperatorBuilder -> Appendable Operator -> FrontieredOperator
/// transforms it into the immutable FrontieredOperator
/// which can be used at runtime
use crate::snapshot::PersistenceBackend;
use crate::time::Timestamp;
use crate::{Key, OperatorId, OperatorPartitioner, WorkerId};

use crate::Data;

/// An Operator which can have output added and can be turned
/// into a FrontieredOperator
/// This trait exists mainly for type erasure, so that the Jetstream
/// need not know the input type of its last operator
pub trait AppendableOperator<K, V, T, P> {
    fn get_output_mut(&mut self) -> &mut Sender<K, V, T, P>;

    fn build(self: Box<Self>) -> FrontieredOperator;
}

/// An Operator which does nothing except passing data along
pub fn pass_through_operator<K: Key, V: Data, T: Timestamp, P: PersistenceBackend>(
) -> StandardOperator<K, V, T, K, V, T, P> {
    StandardOperator::new(|input, output, _ctx| {
        if let Some(x) = input.recv() {
            output.send(x)
        }
    })
}

/// This is a type injected to logic function at runtime
/// and cotains context, whicht the logic generally can not change
/// but utilize
pub struct OperatorContext<'a> {
    pub worker_id: WorkerId,
    pub operator_id: OperatorId,
    pub communication: &'a Postbox<WorkerId>,
}

/// A builder type to build generic operators
pub struct StandardOperator<KI, VI, TI, KO, VO, TO, P> {
    input: Receiver<KI, VI, TI, P>,
    // VODO: get rid of the dynamic dispatch here
    mapper: Box<dyn Mapper<KI, VI, TI, KO, VO, TO, P>>,
    output: Sender<KO, VO, TO, P>,
}

pub trait Mapper<KI, VI, TI, KO, VO, TO, P>:
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
    > Mapper<KI, VI, TI, KO, VO, TO, P> for X
{
}

impl<KI, VI, TI, KO, VO, TO, P> StandardOperator<KI, VI, TI, KO, VO, TO, P>
where
    KI: Key,
    VI: Data,
    KO: Key,
    VO: Data,
    TI: Timestamp,
    TO: Timestamp,
    P: PersistenceBackend,
{
    pub fn new(mapper: impl Mapper<KI, VI, TI, KO, VO, TO, P>) -> Self {
        let input = Receiver::new_unlinked();
        let output = Sender::new_unlinked(full_broadcast);
        Self {
            input,
            mapper: Box::new(mapper),
            output,
        }
    }

    pub fn new_with_output_partitioning(
        mapper: impl Mapper<KI, VI, TI, KO, VO, TO, P>,
        partitioner: impl OperatorPartitioner<KO, VO, TO>,
    ) -> Self {
        let input = Receiver::new_unlinked();
        let output = Sender::new_unlinked(partitioner);
        Self {
            input,
            mapper: Box::new(mapper),
            output,
        }
    }

    pub fn get_input_mut(&mut self) -> &mut Receiver<KI, VI, TI, P> {
        &mut self.input
    }
}

impl<KI, VI, TI, KO, VO, TO, P> AppendableOperator<KO, VO, TO, P>
    for StandardOperator<KI, VI, TI, KO, VO, TO, P>
where
    KI: Key,
    VI: Data,
    TI: Timestamp,
    KO: Key,
    VO: Data,
    TO: Timestamp,
    P: PersistenceBackend,
{
    fn get_output_mut(&mut self) -> &mut Sender<KO, VO, TO, P> {
        &mut self.output
    }

    fn build(self: Box<Self>) -> FrontieredOperator {
        // let operator = StandardOperator{input: self.input, mapper: self.mapper, output: self.output};
        FrontieredOperator::new(*self)
    }
}

/// Type only used internally
// struct StandardOperator<I, O> {
//     input: Receiver<I, P>,
//     mapper: Box<dyn FnMut(&mut Receiver<I, P>, &mut Sender<O, P>, &mut FrontierHandle)>,
//     output: Sender<O, P>,
// }
impl<KI, VI, TI, KO, VO, TO, P> Operator for StandardOperator<KI, VI, TI, KO, VO, TO, P> {
    fn step(&mut self, context: &mut OperatorContext) {
        (self.mapper)(&mut self.input, &mut self.output, context);
    }

    fn has_queued_work(&self) -> bool {
        !self.input.is_empty()
    }
}

pub struct FrontieredOperator {
    operator: Box<dyn Operator>,
}

impl FrontieredOperator {
    fn new<
        KI: Key,
        VI: Data,
        TI: Timestamp,
        KO: Key,
        VO: Data,
        TO: Timestamp,
        P: PersistenceBackend,
    >(
        operator: StandardOperator<KI, VI, TI, KO, VO, TO, P>,
    ) -> Self {
        Self {
            operator: Box::new(operator),
        }
    }

    pub fn build(
        self,
        worker_id: WorkerId,
        operator_id: OperatorId,
        communication: Postbox<WorkerId>,
    ) -> RunnableOperator {
        RunnableOperator {
            worker_id,
            operator_id,
            communication,
            operator: self.operator,
        }
    }
}

pub struct RunnableOperator {
    worker_id: WorkerId,
    operator_id: OperatorId,
    communication: Postbox<WorkerId>,
    operator: Box<dyn Operator>,
}

impl RunnableOperator {
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

// impl<P> RuntimeFrontieredOperator<P> for FrontieredOperator<P>
// where
//     P: PersistenceBackend,
// {

// }

/// The simplest trait in the world
/// Any jetstream operator, MUST implement
/// this trait
pub trait Operator {
    /// Calling step instructs the operator, that it should attempt to make
    /// progress. There is absolutely no assumption on what "progress" means,
    /// but it is implied, that the operator reads its input and writes
    /// to its output
    fn step(&mut self, context: &mut OperatorContext);

    /// still not happy with this function name
    fn has_queued_work(&self) -> bool;
}
// pub trait RuntimeFrontieredOperator<P>
// where
//     P: PersistenceBackend,
// {
//     fn get_probe(&self) -> Probe;

//     fn step(&mut self, operator_id: usize);

//     fn has_queued_work(&self) -> bool;
// }
