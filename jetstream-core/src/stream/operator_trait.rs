use indexmap::IndexSet;

use crate::{
    channels::selective_broadcast::Sender,
    keyed::distributed::{Acquire, Collect, Interrogate},
    snapshot::Barrier,
    DataMessage, ShutdownMarker, WorkerId,
};

use super::operator::{BuildContext, OperatorContext};

pub trait OperatorTrait<KI, VI, TI, KO, VO, TO> {
    fn build(ctx: &BuildContext) -> Self;

    fn schedule(ctx: &OperatorContext, output: &mut Sender<KO, VO, TO>);

    fn data(msg: DataMessage<KI, VI, TI>, ctx: &OperatorContext, output: &mut Sender<KO, VO, TO>);

    fn barrier(msg: Barrier, ctx: &OperatorContext, output: &mut Sender<KO, VO, TO>);

    fn downscale(msg: IndexSet<WorkerId>, ctx: &OperatorContext, output: &mut Sender<KO, VO, TO>);
    fn upscale(msg: IndexSet<WorkerId>, ctx: &OperatorContext, output: &mut Sender<KO, VO, TO>);
    fn shutdown(msg: ShutdownMarker, ctx: &OperatorContext, output: &mut Sender<KO, VO, TO>);

    fn interrogate(msg: Interrogate<KI>, ctx: &OperatorContext, output: &mut Sender<KO, VO, TO>);
    fn collect(msg: Collect<KI>, ctx: &OperatorContext, output: &mut Sender<KO, VO, TO>);
    fn acquire(msg: Acquire<KI>, ctx: &OperatorContext, output: &mut Sender<KO, VO, TO>);
    fn drop_key(msg: KI, ctx: &OperatorContext, output: &mut Sender<KO, VO, TO>);
}
