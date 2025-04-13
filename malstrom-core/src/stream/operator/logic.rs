use crate::{
    channels::operator_io::Output,
    keyed::distributed::{Acquire, Collect, Interrogate},
    snapshot::Barrier,
    types::{DataMessage, MaybeData, MaybeKey, MaybeTime, Message, RescaleMessage, SuspendMarker},
};

use super::{Logic, OperatorContext};

/// This trait provides a way to implement logic without using a closure
/// Usually we would implement this trait on FnMut but unfortunately doing
/// it that way really messes with Rust's type inference and makes closure
/// hard to use as logic directy.
/// Usually it does not make sense to implement this trait directly. Consider using
/// [malstrom::operators::StatefulLogic](StatefulLogic) instead.
pub trait LogicWrapper<K: MaybeKey, VI: MaybeData, TI: MaybeTime, VO: MaybeData, TO: MaybeTime>:
    Sized + 'static
{
    /// Called whenever this operator is scheduled by its worker
    #[allow(unused)]
    fn on_schedule(&mut self, output: &mut Output<K, VO, TO>, ctx: &mut OperatorContext);

    /// Called for every data message reaching the operator
    fn on_data(
        &mut self,
        data_message: DataMessage<K, VI, TI>,
        output: &mut Output<K, VO, TO>,
        ctx: &mut OperatorContext,
    );

    /// Called for every epoch reaching the operator
    fn on_epoch(&mut self, epoch: TI, output: &mut Output<K, VO, TO>, ctx: &mut OperatorContext);

    /// Called for every snapshot barrier reaching the operator
    fn on_barrier(
        &mut self,
        barrier: &mut Barrier,
        output: &mut Output<K, VO, TO>,
        ctx: &mut OperatorContext,
    );

    /// Called whenever a rescale message reaches the operator
    fn on_rescale(
        &mut self,
        rescale_message: &mut RescaleMessage,
        output: &mut Output<K, VO, TO>,
        ctx: &mut OperatorContext,
    );

    /// Called when the SuspendMarker reaches the operator. This indicates the job will shutdown,
    /// even though execution is not finished.
    /// The operator will not be scheduled again after this until the job is restarted.
    fn on_suspend(
        &mut self,
        suspend_marker: &mut SuspendMarker,
        output: &mut Output<K, VO, TO>,
        ctx: &mut OperatorContext,
    );

    /// Called when a key interrogation message reaches the operator.
    /// The operator must inform the interrogation message about all keys it currently
    /// holds in state
    fn on_interrogate(
        &mut self,
        interrogate: &mut Interrogate<K>,
        output: &mut Output<K, VO, TO>,
        ctx: &mut OperatorContext,
    );

    /// Called when a key-state collection message reaches the operator.
    /// The operator must hand the state for the given key to the collection message.
    /// No more messages of the given key will reach the operator after this message
    fn on_collect(
        &mut self,
        collect: &mut Collect<K>,
        output: &mut Output<K, VO, TO>,
        ctx: &mut OperatorContext,
    );

    /// Called when a key-state acquire message reaches the operator.
    /// The operator must take the state given by the acquire message and add it to its local key
    /// state.
    fn on_acquire(
        &mut self,
        acquire: &mut Acquire<K>,
        output: &mut Output<K, VO, TO>,
        ctx: &mut OperatorContext,
    );

    /// Turn this type into a schedulable function which can be scheduled by the Malstrom worker.
    fn into_logic(mut self) -> impl Logic<K, VI, TI, K, VO, TO> {
        move |input, output, ctx| {
            self.on_schedule(output, ctx);
            let msg = match input.recv() {
                Some(x) => x,
                None => return,
            };
            match msg {
                Message::Data(data_message) => self.on_data(data_message, output, ctx),
                Message::Epoch(epoch) => self.on_epoch(epoch, output, ctx),
                Message::AbsBarrier(mut barrier) => {
                    self.on_barrier(&mut barrier, output, ctx);
                    output.send(barrier.into());
                }
                Message::Rescale(mut rescale_message) => {
                    self.on_rescale(&mut rescale_message, output, ctx);
                    output.send(rescale_message.into());
                }
                Message::SuspendMarker(mut suspend_marker) => {
                    self.on_suspend(&mut suspend_marker, output, ctx);
                    output.send(suspend_marker.into());
                }
                Message::Interrogate(mut interrogate) => {
                    self.on_interrogate(&mut interrogate, output, ctx);
                    output.send(interrogate.into());
                }
                Message::Collect(mut collect) => {
                    self.on_collect(&mut collect, output, ctx);
                    output.send(collect.into());
                }
                Message::Acquire(mut acquire) => {
                    self.on_acquire(&mut acquire, output, ctx);
                    output.send(acquire.into());
                }
            };
        }
    }
}
