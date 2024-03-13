use indexmap::IndexSet;

use crate::{
    channels::selective_broadcast::Sender, snapshot::{Barrier, Load}, stream::{
        jetstream::JetStreamBuilder,
        operator::{BuildContext, OperatorBuilder, OperatorContext},
    }, time::{Epoch, MaybeTime, NoTime}, Data, DataMessage, MaybeKey, Message, NoData, NoKey, ShutdownMarker, WorkerId
};

pub trait DataSourceBuilder<P>: 'static {
    type Source;
    fn build(self, ctx: &BuildContext<P>) -> Self::Source;
    
}

pub trait DataSource<K, V, T, P>: 'static {
    fn give(&mut self, ctx: &OperatorContext, collector: &mut DataCollector<K, V, T, P>) -> ();
    fn handle(&mut self, sys_message: &mut SystemMessage<P>) -> ();
}



pub struct DataCollector<'a, K, V, T, P> {
    sender: &'a mut Sender<K, V, T, P>
}
impl<'a, K, V, T, P> DataCollector<'a, K, V, T, P> where K: Clone, V: Clone, T: Clone {
    fn new(sender: &'a mut Sender<K, V, T, P>) -> Self {
        Self { sender }
    }

    pub fn send_data(&mut self, key: K, value: V, timestamp: T) -> () {
        self.sender.send(Message::Data(DataMessage::new(key, value, timestamp)))
    }
    pub fn send_epoch(&mut self, timestamp: T) -> () {
        self.sender.send(Message::Epoch(Epoch::new(timestamp)))
    }
}
pub trait Source<K, V, T, P, D> {
    fn source(self, source: impl DataSourceBuilder<P, Source = D>) -> JetStreamBuilder<K, V, T, P>;
}

impl<K, V, T, P, D> Source<K, V, T, P, D> for JetStreamBuilder<NoKey, NoData, NoTime, P>
where
    K: MaybeKey,
    V: Data,
    T: MaybeTime,
    P: 'static,
    D:  DataSource<K, V, T, P>
{
    fn source(self, source: impl DataSourceBuilder<P, Source = D>) -> JetStreamBuilder<K, V, T, P>{
        let operator = OperatorBuilder::built_by(move |build_context: &BuildContext<P>| {
            let mut built_source = source.build(build_context);
            
            move |input, output, ctx| {
                let mut collector = DataCollector::new(output);
                built_source.give(ctx, &mut collector);
                if let Some(msg) = input.recv() {
                    built_source.handle(&mut msg.into())
                }
            }
        });
        self.then(operator)
    }
}

/// Same as the normal jetstream messages but without Epochs, Data,
/// or key related messages, since those can not reach the source.
#[derive(Debug)]
pub enum SystemMessage<P> {
    /// Barrier used for asynchronous snapshotting
    AbsBarrier(Barrier<P>),
    /// Instruction to load state
    Load(Load<P>),
    /// Information that the worker of this ID will soon be removed
    /// from the computation. Triggers Rescaling procedure
    ScaleRemoveWorker(IndexSet<WorkerId>),
    /// Information that the worker of this ID will be added to the
    /// computation. Triggers Rescaling procedure
    ScaleAddWorker(IndexSet<WorkerId>),
    /// Information that this worker plans on shutting down
    /// See struct docstring for more information
    ShutdownMarker(ShutdownMarker),
}
impl<P> From<Message<NoKey, NoData, NoTime, P>> for SystemMessage<P> {
    fn from(value: Message<NoKey, NoData, NoTime, P>) -> Self {
        match value {
            Message::Data(_) => unreachable!(),
            Message::Epoch(_) => unreachable!(),
            Message::AbsBarrier(x) => Self::AbsBarrier(x),
            Message::Load(x) => Self::Load(x),
            Message::ScaleRemoveWorker(x) => Self::ScaleRemoveWorker(x),
            Message::ScaleAddWorker(x) => Self::ScaleAddWorker(x),
            Message::ShutdownMarker(x) => Self::ShutdownMarker(x),
            Message::Interrogate(_) => unreachable!(),
            Message::Collect(_) => unreachable!(),
            Message::Acquire(_) => unreachable!(),
            Message::DropKey(_) => unreachable!(),
        }
    }
}

/// Only epochs and data
#[derive(Debug)]
pub enum SourceMessage<K, V, T> {
    Data(DataMessage<K, V, T>),
    Epoch(Epoch<T>)
}

impl<K, V, T, P> Into<Message<K, V, T, P>> for SourceMessage<K, V, T> {
    fn into(self) -> Message<K, V, T, P> {
        match self {
            SourceMessage::Data(x) => Message::Data(x),
            SourceMessage::Epoch(x) => Message::Epoch(x),
        }
    }
}