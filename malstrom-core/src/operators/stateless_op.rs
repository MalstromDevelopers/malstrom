use std::marker::PhantomData;

use crate::{
    channels::operator_io::{Input, Output},
    stream::{DirectLogic, Logic, Malstrom, Operator, SafeLogic, SafeLogicWrapper, StreamBuilder},
    types::{Data, DataMessage, Kvt, MaybeKey, Message, Sealed, Timestamp},
};

/// A custom stateless operator for Malstrom streams
pub trait StatelessLogic<In: Kvt, T: Data>: 'static {
    /// Return Some to retain the key-state and None to discard it
    async fn on_data(
        &mut self,
        msg: DataMessage<In>,
        output: &mut Output<(In::Key, T, In::Timestamp)>,
    );

    /// Handle an incoming epoch. The default implementation is a no-op
    async fn on_epoch(
        &mut self,
        _epoch: &<In as Kvt>::Timestamp,
        _output: &mut Output<(In::Key, T, In::Timestamp)>,
    ) {
    }
}

impl<X, Fut, In, T> StatelessLogic<In, T> for X
where
    In: Kvt,
    T: Data,
    Fut: Future,
    X: (FnMut(DataMessage<In>, &mut Output<(In::Key, T, In::Timestamp)>) -> Fut) + 'static,
{
    async fn on_data(
        &mut self,
        msg: DataMessage<In>,
        output: &mut Output<(In::Key, T, In::Timestamp)>,
    ) {
        self(msg, output).await;
    }
}

/// Add a custom stateless operator to the stream. See [StatelessLogic] for how to implement a
/// custom stateless operator
pub trait StatelessOp<In, T, L>: Sealed
where
    In: Kvt,
    T: Data,
    L: StatelessLogic<In, T>,
{
    /// A small wrapper around StandardOperator to make allow simpler
    /// implementations of stateless, time-unaware operators like map or filter
    ///
    /// The mapper is only called for data messages, all other messages are passed
    /// along as they are.
    fn stateless_op(
        self,
        name: impl Into<String>,
        logic: L,
    ) -> StreamBuilder<(In::Key, T, In::Timestamp)>;
}

type StatelessOperator<In: Kvt, T, L> = Operator<In, DirectLogic<L>, (In::Key, T, In::Timestamp)>;

impl<In, T, L, X> StatelessOp<In, T, L> for X
where
    X: Malstrom<In>,
    In: Kvt,
    T: Data,
    L: StatelessLogic<In, T>,
{
    fn stateless_op(
        self,
        name: impl Into<String>,
        logic: L,
    ) -> StreamBuilder<(In::Key, T, In::Timestamp)> {
        let op = Operator::direct(
            name.into(),
            StatelessOperatorImpl {
                logic,
                _input: PhantomData::<In>,
                _output: PhantomData::<T>,
            }
            .into_logic(),
        );
        self.then(op)
    }
}

struct StatelessOperatorImpl<In, T, L> {
    logic: L,
    _input: PhantomData<In>,
    _output: PhantomData<T>,
}

impl<L, In, T> SafeLogic<In, (In::Key, T, In::Timestamp)> for StatelessOperatorImpl<In, T, L>
where
    In: Kvt,
    T: Data,
    L: StatelessLogic<In, T>,
{
    async fn on_schedule(
        &mut self,
        output: &mut Output<(In::Key, T, In::Timestamp)>,
        ctx: &mut crate::stream::OperatorContext,
    ) {
    }

    async fn on_data(
        &mut self,
        data_message: DataMessage<In>,
        output: &mut Output<(In::Key, T, In::Timestamp)>,
        ctx: &mut crate::stream::OperatorContext,
    ) {
        (self.logic).on_data(data_message, output).await;
    }

    async fn on_epoch(
        &mut self,
        epoch: &<In as Kvt>::Timestamp,
        output: &mut Output<(In::Key, T, In::Timestamp)>,
        ctx: &mut crate::stream::OperatorContext,
    ) {
        (self.logic).on_epoch(&epoch, output).await;
    }
}
