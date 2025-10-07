use std::marker::PhantomData;

use crate::{
    channels::operator_io::{Input, Output},
    stream::{DirectLogic, Logic, Malstrom, Operator, SafeLogic, SafeLogicWrapper, StreamBuilder},
    types::{Data, DataMessage, Kvt, MaybeKey, Message, Sealed, Timestamp},
};

/// A custom stateless operator for Malstrom streams
pub trait StatelessLogic<M: Kvt, N: Kvt<Key = M::Key, Timestamp = M::Timestamp>>: 'static {
    /// Return Some to retain the key-state and None to discard it
    fn on_data(&mut self, msg: DataMessage<M>, output: &mut Output<N>);

    /// Handle an incoming epoch. The default implementation is a no-op
    fn on_epoch(&mut self, _epoch: &<M as Kvt>::Timestamp, _output: &mut Output<N>) {}
}

impl<X, M, N> StatelessLogic<M, N> for X
where
    M: Kvt,
    N: Kvt<Key = M::Key, Timestamp = M::Timestamp>,
    X: FnMut(DataMessage<M>, &mut Output<N>) + 'static,
{
    fn on_data(&mut self, msg: DataMessage<M>, output: &mut Output<N>) {
        self(msg, output);
    }
}

/// Add a custom stateless operator to the stream. See [StatelessLogic] for how to implement a
/// custom stateless operator
pub trait StatelessOp<M, N>: Sealed where M: Kvt, N: Kvt<Key = M::Key, Timestamp = M::Timestamp>{
    /// A small wrapper around StandardOperator to make allow simpler
    /// implementations of stateless, time-unaware operators like map or filter
    ///
    /// The mapper is only called for data messages, all other messages are passed
    /// along as they are.
    fn stateless_op<L: StatelessLogic<M, N>>(
        self,
        name: impl Into<String>,
        logic: L,
    ) -> StreamBuilder<N>;
}

type StatelessOperator<M, N, L> = Operator<M, DirectLogic<L>, N>;

impl<M, N, X> StatelessOp<M, N> for X
where
    X: Malstrom<M>,
    M: Kvt,
    N: Kvt<Key = M::Key, Timestamp = M::Timestamp>,
{
    fn stateless_op<L: StatelessLogic<M, N>>(
        self,
        name: impl Into<String>,
        logic: L,
    ) -> StreamBuilder<N> {
        let op = Operator::direct(
            name.into(),
            StatelessOperatorImpl {
                logic,
                _input: PhantomData::<M>,
                _output: PhantomData::<N>,
            }.into_logic()
        );
        self.then(op)
    }
}

struct StatelessOperatorImpl<M, N, L> {
    logic: L,
    _input: PhantomData<M>,
    _output: PhantomData<N>,
}

impl<L, M, N> SafeLogic<M, N> for StatelessOperatorImpl<M, N, L>
where
    M: Kvt,
    N: Kvt<Key = M::Key, Timestamp = M::Timestamp>,
    L: StatelessLogic<M, N>,
{
    fn on_schedule(&mut self, output: &mut Output<N>, ctx: &mut crate::stream::OperatorContext) {}

    fn on_data(
        &mut self,
        data_message: DataMessage<M>,
        output: &mut Output<N>,
        ctx: &mut crate::stream::OperatorContext,
    ) {
        (self.logic).on_data(data_message, output);
    }

    fn on_epoch(
        &mut self,
        epoch: <M as Kvt>::Timestamp,
        output: &mut Output<N>,
        ctx: &mut crate::stream::OperatorContext,
    ) {
        (self.logic).on_epoch(&epoch, output);
        output.send(Message::Epoch(epoch));
    }

    fn on_barrier(
        &mut self,
        barrier: &mut crate::snapshot::Barrier,
        output: &mut Output<N>,
        ctx: &mut crate::stream::OperatorContext,
    ) {
    }

    fn on_rescale(
        &mut self,
        rescale_message: &mut crate::types::RescaleMessage,
        output: &mut Output<N>,
        ctx: &mut crate::stream::OperatorContext,
    ) {
    }

    fn on_suspend(
        &mut self,
        suspend_marker: &mut crate::types::SuspendMarker,
        output: &mut Output<N>,
        ctx: &mut crate::stream::OperatorContext,
    ) {
    }

    fn on_interrogate(
        &mut self,
        interrogate: &mut crate::keyed::distributed::Interrogate<<M as Kvt>::Key>,
        output: &mut Output<N>,
        ctx: &mut crate::stream::OperatorContext,
    ) {
    }

    fn on_collect(
        &mut self,
        collect: &mut crate::keyed::distributed::Collect<<M as Kvt>::Key>,
        output: &mut Output<N>,
        ctx: &mut crate::stream::OperatorContext,
    ) {
    }

    fn on_acquire(
        &mut self,
        acquire: &mut crate::keyed::distributed::Acquire<<M as Kvt>::Key>,
        output: &mut Output<N>,
        ctx: &mut crate::stream::OperatorContext,
    ) {
    }
}
