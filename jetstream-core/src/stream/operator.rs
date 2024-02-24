use postbox::Postbox;

use crate::channels::selective_broadcast::{full_broadcast, Receiver, Sender};
/// Operators:
/// Lifecycle:
/// OperatorBuilder -> Appendable Operator -> FrontieredOperator
/// transforms it into the immutable FrontieredOperator
/// which can be used at runtime
use crate::frontier::{Frontier, FrontierHandle, Probe, Timestamp};
use crate::snapshot::PersistenceBackend;
use crate::{Key, OperatorId, OperatorPartitioner, WorkerId};

use crate::Data;

/// An Operator which can have output added and can be turned
/// into a FrontieredOperator
/// This trait exists mainly for type erasure, so that the Jetstream
/// need not know the input type of its last operator
pub trait AppendableOperator<K, T, P: PersistenceBackend> {
    fn get_output_mut(&mut self) -> &mut Sender<K, T, P>;

    fn build(self: Box<Self>) -> FrontieredOperator<P>;
}

/// An Operator which does nothing except passing data along
pub fn pass_through_operator<K: Key, T: Data, P: PersistenceBackend>(
) -> StandardOperator<K, T, K, T, P> {
    StandardOperator::new(|input, output, ctx| {
        ctx.frontier.advance_to(Timestamp::MAX);
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
    pub frontier: FrontierHandle<'a>,
    pub communication: &'a Postbox<WorkerId>,
}

/// A builder type to build generic operators
pub struct StandardOperator<KI, TI, KO, TO, P: PersistenceBackend> {
    input: Receiver<KI, TI, P>,
    // TODO: get rid of the dynamic dispatch here
    mapper: Box<dyn Mapper<KI, TI, KO, TO, P>>,
    output: Sender<KO, TO, P>,
}

pub trait Mapper<KI, TI, KO, TO, P>:
    FnMut(&mut Receiver<KI, TI, P>, &mut Sender<KO, TO, P>, &mut OperatorContext) + 'static
{
}
impl<
        KI,
        TI,
        KO,
        TO,
        P,
        T: FnMut(&mut Receiver<KI, TI, P>, &mut Sender<KO, TO, P>, &mut OperatorContext) + 'static,
    > Mapper<KI, TI, KO, TO, P> for T
{
}

impl<KI, TI, KO, TO, P> StandardOperator<KI, TI, KO, TO, P>
where
    KI: Key,
    TI: Data,
    KO: Key,
    TO: Data,
    P: PersistenceBackend,
{
    pub fn new(mapper: impl Mapper<KI, TI, KO, TO, P>) -> Self {
        let input = Receiver::new_unlinked();
        let output = Sender::new_unlinked(full_broadcast);
        Self {
            input,
            mapper: Box::new(mapper),
            output,
        }
    }

    pub fn new_with_output_partitioning(
        mapper: impl Mapper<KI, TI, KO, TO, P>,
        partitioner: impl OperatorPartitioner<KO, TO>,
    ) -> Self {
        let input = Receiver::new_unlinked();
        let output = Sender::new_unlinked(partitioner);
        Self {
            input,
            mapper: Box::new(mapper),
            output,
        }
    }

    pub fn get_input_mut(&mut self) -> &mut Receiver<KI, TI, P> {
        &mut self.input
    }
}

impl<KI, TI, KO, TO, P> AppendableOperator<KO, TO, P> for StandardOperator<KI, TI, KO, TO, P>
where
    KI: Key,
    TI: Data,
    KO: Key,
    TO: Data,
    P: PersistenceBackend,
{
    fn get_output_mut(&mut self) -> &mut Sender<KO, TO, P> {
        &mut self.output
    }

    fn build(self: Box<Self>) -> FrontieredOperator<P> {
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
impl<KI, TI, KO, TO, P> Operator<P> for StandardOperator<KI, TI, KO, TO, P>
where
    //     O: Clone,
    P: PersistenceBackend,
{
    fn step(&mut self, context: &mut OperatorContext) {
        (self.mapper)(&mut self.input, &mut self.output, context);
    }

    fn has_queued_work(&self) -> bool {
        !self.input.is_empty()
    }
}

pub struct FrontieredOperator<P> {
    frontier: Frontier,
    operator: Box<dyn Operator<P>>,
}

impl<P> FrontieredOperator<P>
where
    P: PersistenceBackend,
{
    fn new<KI: Key, KT: Data, KO: Key, TO: Data>(
        operator: StandardOperator<KI, KT, KO, TO, P>,
    ) -> Self {
        Self {
            frontier: Frontier::default(),
            operator: Box::new(operator),
        }
    }

    pub fn add_upstream_probe(&mut self, probe: Probe) {
        self.frontier.add_upstream_probe(probe)
    }

    pub fn get_probe(&self) -> Probe {
        self.frontier.get_probe()
    }

    pub fn build(
        self,
        worker_id: WorkerId,
        operator_id: OperatorId,
        communication: Postbox<WorkerId>,
    ) -> RunnableOperator<P> {
        RunnableOperator {
            worker_id,
            operator_id,
            frontier: self.frontier,
            communication,
            operator: self.operator,
        }
    }
}

pub struct RunnableOperator<P> {
    worker_id: WorkerId,
    operator_id: OperatorId,
    frontier: Frontier,
    communication: Postbox<WorkerId>,
    operator: Box<dyn Operator<P>>,
}

impl<P> RunnableOperator<P>
where
    P: PersistenceBackend,
{
    pub fn step(&mut self) {
        let mut context = OperatorContext {
            worker_id: self.worker_id,
            operator_id: self.operator_id,
            frontier: FrontierHandle::new(&mut self.frontier),
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
pub trait Operator<P: PersistenceBackend> {
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
