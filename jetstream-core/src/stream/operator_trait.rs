use indexmap::IndexSet;

use crate::{
    channels::selective_broadcast::Sender,
    keyed::distributed::messages::{Acquire, Collect, Interrogate},
    snapshot::Barrier,
    DataMessage, ShutdownMarker, WorkerId,
};

use super::operator::{BuildContext, OperatorContext};

pub trait OperatorTrait<KI, VI, TI, KO, VO, TO, P> {
    fn build(ctx: &BuildContext<P>) -> Self;

    fn schedule(ctx: &OperatorContext, output: &mut Sender<KO, VO, TO, P>);

    fn data(
        msg: DataMessage<KI, VI, TI>,
        ctx: &OperatorContext,
        output: &mut Sender<KO, VO, TO, P>,
    );

    fn barrier(msg: Barrier<P>, ctx: &OperatorContext, output: &mut Sender<KO, VO, TO, P>);

    fn downscale(
        msg: IndexSet<WorkerId>,
        ctx: &OperatorContext,
        output: &mut Sender<KO, VO, TO, P>,
    );
    fn upscale(msg: IndexSet<WorkerId>, ctx: &OperatorContext, output: &mut Sender<KO, VO, TO, P>);
    fn shutdown(msg: ShutdownMarker, ctx: &OperatorContext, output: &mut Sender<KO, VO, TO, P>);

    fn interrogate(msg: Interrogate<KI>, ctx: &OperatorContext, output: &mut Sender<KO, VO, TO, P>);
    fn collect(msg: Collect<KI>, ctx: &OperatorContext, output: &mut Sender<KO, VO, TO, P>);
    fn acquire(msg: Acquire<KI>, ctx: &OperatorContext, output: &mut Sender<KO, VO, TO, P>);
    fn drop_key(msg: KI, ctx: &OperatorContext, output: &mut Sender<KO, VO, TO, P>);
}
