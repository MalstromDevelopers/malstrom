use std::marker::PhantomData;

use crate::{
    channels::operator_io::{Input, Output},
    keyed::distributed::{Acquire, Collect, Interrogate},
    snapshot::Barrier,
    types::{
        DataMessage, Kvt, MaybeData, MaybeKey, MaybeTime, Message, RescaleMessage, SuspendMarker,
    },
};

use super::OperatorContext;

/// Operator Logic with absolutely no safeguard, allows you to break keying and everything else
pub(crate) trait Logic<M: Kvt, N: Kvt>: 'static {
    async fn apply(
        &mut self,
        input: &mut Input<M>,
        output: &mut Output<N>,
        ctx: &mut OperatorContext,
    );
}

/// This trait provides a way to implement logic with no risk of breaking internal messaging invariants.
/// Usually it does not make sense to implement this trait directly. Consider using
/// [malstrom::operators::StatefulLogic](StatefulLogic) instead.
pub trait SafeLogic<M: Kvt, N: Kvt<Key = M::Key>>: Sized + 'static {
    /// Called whenever this operator is scheduled by its worker
    async fn on_schedule(&mut self, output: &mut Output<N>, ctx: &mut OperatorContext<'_>) {}

    /// Called for every data message reaching the operator
    async fn on_data(
        &mut self,
        data_message: DataMessage<M>,
        output: &mut Output<N>,
        ctx: &mut OperatorContext<'_>,
    );

    /// Called for every epoch reaching the operator
    async fn on_epoch(
        &mut self,
        epoch: &<M as Kvt>::Timestamp,
        output: &mut Output<N>,
        ctx: &mut OperatorContext<'_>,
    ) {
    }

    /// Called for every snapshot barrier reaching the operator
    async fn on_barrier(
        &mut self,
        barrier: &mut Barrier,
        output: &mut Output<N>,
        ctx: &mut OperatorContext<'_>,
    ) {
    }

    /// Called whenever a rescale message reaches the operator
    async fn on_rescale(
        &mut self,
        rescale_message: &mut RescaleMessage,
        output: &mut Output<N>,
        ctx: &mut OperatorContext<'_>,
    ) {
    }

    /// Called when the SuspendMarker reaches the operator. This indicates the job will shutdown,
    /// even though execution is not finished.
    /// The operator will not be scheduled again after this until the job is restarted.
    async fn on_suspend(
        &mut self,
        suspend_marker: &mut SuspendMarker,
        output: &mut Output<N>,
        ctx: &mut OperatorContext<'_>,
    ) {
    }

    /// Called when a key interrogation message reaches the operator.
    /// The operator must inform the interrogation message about all keys it currently
    /// holds in state
    async fn on_interrogate(
        &mut self,
        interrogate: &mut Interrogate<<M as Kvt>::Key>,
        output: &mut Output<N>,
        ctx: &mut OperatorContext<'_>,
    ) {
    }

    /// Called when a key-state collection message reaches the operator.
    /// The operator must hand the state for the given key to the collection message.
    /// No more messages of the given key will reach the operator after this message
    async fn on_collect(
        &mut self,
        collect: &mut Collect<<M as Kvt>::Key>,
        output: &mut Output<N>,
        ctx: &mut OperatorContext<'_>,
    ) {
    }

    /// Called when a key-state acquire message reaches the operator.
    /// The operator must take the state given by the acquire message and add it to its local key
    /// state.
    async fn on_acquire(
        &mut self,
        acquire: &mut Acquire<<M as Kvt>::Key>,
        output: &mut Output<N>,
        ctx: &mut OperatorContext<'_>,
    ) {
    }

    /// Turn this type into a schedulable function which can be scheduled by the Malstrom worker.
    fn into_logic(self) -> SafeLogicWrapper<Self> {
        SafeLogicWrapper {
            implementation: self,
        }
    }
}

// impl<In, Out, X> Logic<In, Out> for X where X: SafeLogic<In, Out>, In: Kvt, Out: Kvt {
//     async fn apply(
//         &mut self,
//         input: &mut Input<In>,
//         output: &mut Output<Out>,
//         ctx: &mut OperatorContext<'_>,
//     ) {
//         todo!()
//     }
// }

pub struct SafeLogicWrapper<L> {
    implementation: L,
}

impl<M, N, L> Logic<M, N> for SafeLogicWrapper<L>
where
    M: Kvt,
    N: Kvt<Key = M::Key, Timestamp = M::Timestamp>,
    L: SafeLogic<M, N>,
{
    async fn apply(
        &mut self,
        input: &mut Input<M>,
        output: &mut Output<N>,
        ctx: &mut OperatorContext<'_>,
    ) {
        self.implementation.on_schedule(output, ctx);
        let msg = match input.recv() {
            Some(x) => x,
            None => return,
        };
        match msg {
            Message::Data(data_message) => {
                self.implementation.on_data(data_message, output, ctx).await
            }
            Message::Epoch(epoch) => {
                self.implementation.on_epoch(&epoch, output, ctx).await;
                output.send(Message::Epoch(epoch));
            }
            Message::AbsBarrier(mut barrier) => {
                self.implementation
                    .on_barrier(&mut barrier, output, ctx)
                    .await;
                output.send(barrier.into());
            }
            Message::Rescale(mut rescale_message) => {
                self.implementation
                    .on_rescale(&mut rescale_message, output, ctx)
                    .await;
                output.send(rescale_message.into());
            }
            Message::SuspendMarker(mut suspend_marker) => {
                self.implementation
                    .on_suspend(&mut suspend_marker, output, ctx)
                    .await;
                output.send(suspend_marker.into());
            }
            Message::Interrogate(mut interrogate) => {
                self.implementation
                    .on_interrogate(&mut interrogate, output, ctx)
                    .await;
                output.send(interrogate.into());
            }
            Message::Collect(mut collect) => {
                self.implementation
                    .on_collect(&mut collect, output, ctx)
                    .await;
                output.send(collect.into());
            }
            Message::Acquire(mut acquire) => {
                self.implementation
                    .on_acquire(&mut acquire, output, ctx)
                    .await;
                output.send(acquire.into());
            }
        };
    }
}
