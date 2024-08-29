use indexmap::IndexSet;

use crate::{
    channels::selective_broadcast::{Receiver, Sender}, keyed::distributed::{Acquire, Collect, Interrogate}, snapshot::Barrier, time::MaybeTime, DataMessage, Message, RescaleMessage, ShutdownMarker, WorkerId
};

use super::operator::{BuildContext, OperatorContext};

pub trait SingleInputOperator<KI, VI, TI: MaybeTime, KO: Clone, VO: Clone, TO: MaybeTime>: EpochHandler<TI, KO, VO, TO> + StatefulOperator<KI, KO, VO, TO> {
    const BUILDER: fn(&BuildContext) -> Self;

    fn schedule(&mut self, ctx: &OperatorContext, input: &mut Receiver<KI, VI, TI>, output: &mut Sender<KO, VO, TO>) -> (){
        let msg = match input.recv() {
            Some(x) => x,
            None => return
        };
        match msg {
            crate::Message::Data(d) => self.data(d, ctx, output),
            crate::Message::Epoch(e) => self.epoch(e, ctx, output),
            crate::Message::AbsBarrier(b) => self.barrier(b, ctx, output),
            crate::Message::Rescale(r) => self.rescale(r, ctx, output),
            crate::Message::ShutdownMarker(s) => self.shutdown(s, ctx, output),
            crate::Message::Interrogate(i) => self.interrogate(i, ctx, output),
            crate::Message::Collect(c) => self.collect(c, ctx, output),
            crate::Message::Acquire(a) => self.acquire(a, ctx, output),
            crate::Message::DropKey(d) => self.drop_key(d, ctx, output)
        };
    }

    fn data(&mut self, msg: DataMessage<KI, VI, TI>, ctx: &OperatorContext, output: &mut Sender<KO, VO, TO>);
    
    fn rescale(&mut self, msg: RescaleMessage, _: &OperatorContext, output: &mut Sender<KO, VO, TO>){
        output.send(Message::Rescale(msg))
    }
    fn shutdown(&mut self, msg: ShutdownMarker, _: &OperatorContext, output: &mut Sender<KO, VO, TO>){
        output.send(Message::ShutdownMarker(msg))
    }
}

pub trait EpochHandler<TI, KO, VO, TO> {
    fn epoch(&mut self, msg: TI, ctx: &OperatorContext, output: &mut Sender<KO, VO, TO>);
}
pub trait EpochForward {}
impl <X, KO, VO, T> EpochHandler<T, KO, VO, T> for X where X: EpochForward, KO: Clone, VO: Clone, T: MaybeTime {
    fn epoch(&mut self, msg: T, _: &OperatorContext, output: &mut Sender<KO, VO, T>) {
        output.send(Message::Epoch(msg))
    }
}

pub trait StatefulOperator<KI, KO, VO, TO> {
    fn barrier(&mut self, msg: Barrier, _: &OperatorContext, output: &mut Sender<KO, VO, TO>);
    fn interrogate(&mut self, msg: Interrogate<KI>, ctx: &OperatorContext, output: &mut Sender<KO, VO, TO>);
    fn collect(&mut self, msg: Collect<KI>, ctx: &OperatorContext, output: &mut Sender<KO, VO, TO>);
    fn acquire(&mut self, msg: Acquire<KI>, ctx: &OperatorContext, output: &mut Sender<KO, VO, TO>);
    fn drop_key(&mut self, msg: KI, ctx: &OperatorContext, output: &mut Sender<KO, VO, TO>);
}

pub trait StatelessOperator<K, VO, TO>{}
impl <X, K, VO, TO> StatefulOperator<K, K, VO, TO> for X where X: StatelessOperator<K, VO, TO>, K: Clone, VO: Clone, TO: MaybeTime{
    fn barrier(&mut self, msg: Barrier, _: &OperatorContext, output: &mut Sender<K, VO, TO>) {
        output.send(Message::AbsBarrier(msg))
    }

    fn interrogate(&mut self, msg: Interrogate<K>, _: &OperatorContext, output: &mut Sender<K, VO, TO>) {
        output.send(Message::Interrogate(msg))
    }

    fn collect(&mut self, msg: Collect<K>, _: &OperatorContext, output: &mut Sender<K, VO, TO>) {
        output.send(Message::Collect(msg))
    }

    fn acquire(&mut self, msg: Acquire<K>, _: &OperatorContext, output: &mut Sender<K, VO, TO>) {
        output.send(Message::Acquire(msg))
    }

    fn drop_key(&mut self, msg: K, _: &OperatorContext, output: &mut Sender<K, VO, TO>) {
        output.send(Message::DropKey(msg))
    }
}