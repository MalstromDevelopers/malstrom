use crate::{
    channels::operator_io::Output,
    keyed::distributed::{Acquire, Collect, Interrogate},
    snapshot::Barrier,
    types::{
        DataMessage, MaybeData, MaybeKey, MaybeTime, Message,
        RescaleMessage, SuspendMarker,
    },
};

use super::{
    Logic,
    OperatorContext,
};

/// This trait provides a way to implement logic without using a closure
/// Usually we would implement this trait on FnMut but unfortunately doing
/// it that way really messes with Rust's type inference and makes closure
/// hard to use as logic directy
pub trait LogicWrapper<K: MaybeKey, VI: MaybeData, TI: MaybeTime, VO: MaybeData, TO: MaybeTime>:
    Sized + 'static
{
    #[allow(unused)]
    fn on_schedule(&mut self, output: &mut Output<K, VO, TO>, ctx: &mut OperatorContext) -> ();

    fn on_data(
        &mut self,
        data_message: DataMessage<K, VI, TI>,
        output: &mut Output<K, VO, TO>,
        ctx: &mut OperatorContext,
    ) -> ();

    fn on_epoch(
        &mut self,
        epoch: TI,
        output: &mut Output<K, VO, TO>,
        ctx: &mut OperatorContext,
    ) -> ();

    fn on_barrier(
        &mut self,
        barrier: &mut Barrier,
        output: &mut Output<K, VO, TO>,
        ctx: &mut OperatorContext,
    ) -> ();

    fn on_rescale(
        &mut self,
        rescale_message: &mut RescaleMessage,
        output: &mut Output<K, VO, TO>,
        ctx: &mut OperatorContext,
    ) -> ();

    fn on_suspend(
        &mut self,
        suspend_marker: &mut SuspendMarker,
        output: &mut Output<K, VO, TO>,
        ctx: &mut OperatorContext,
    ) -> ();

    fn on_interrogate(
        &mut self,
        interrogate: &mut Interrogate<K>,
        output: &mut Output<K, VO, TO>,
        ctx: &mut OperatorContext,
    ) -> ();

    fn on_collect(
        &mut self,
        collect: &mut Collect<K>,
        output: &mut Output<K, VO, TO>,
        ctx: &mut OperatorContext,
    ) -> ();

    fn on_acquire(
        &mut self,
        acquire: &mut Acquire<K>,
        output: &mut Output<K, VO, TO>,
        ctx: &mut OperatorContext,
    ) -> ();

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
