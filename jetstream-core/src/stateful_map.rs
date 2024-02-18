use crate::{
    channels::selective_broadcast::Receiver,
    frontier::FrontierHandle,
    snapshot::{barrier::BarrierData, PersistenceBackend},
    stream::{
        jetstream::JetStreamBuilder,
        operator::StandardOperator,
    }, DataMessage, Message,
};

pub trait StatefulMap<K, TI, P: PersistenceBackend> {
    fn stateful_map<K, TO, S: Default + 'static>(
        self,
        mapper: impl FnMut(DataMessage<K, TI>, &mut FrontierHandle, &mut S) -> DataMessage<K, TO> + 'static,
    ) -> JetStreamBuilder<K, TO, P>;
}

impl<KI, TI, P> StatefulMap<KI, TI, P> for JetStreamBuilder<KI, TI, P>
where
    P: PersistenceBackend,
{
    fn stateful_map<KO, TO, S: Default + 'static>(
        self,
        mut mapper: impl FnMut(DataMessage<KI, TI>, &mut FrontierHandle, &mut S) -> DataMessage<KO, TO> + 'static,
    ) -> JetStreamBuilder<KO, TO, P> {
        let mut state: Option<S> = None;
        let operator = StandardOperator::new(move |input: &mut Receiver<KI, TI, P>, output, ctx| {
            let msg = match input.recv() {
                Some(x) => x,
                None => return
            };
            match msg {
                Message::Load(l) => {
                    let (time, loaded_state) = l.load(ctx.operator_id).unwrap_or_default();
                    ctx.frontier.advance_to(time);
                    state = Some(loaded_state);
                }
                Message::AbsBarrier(mut b) => {
                    if let Some(st) = state.as_ref() {
                        b.persist(ctx.frontier.get_actual(), &st, ctx.operator_id);
                    }
                    output.send(BarrierData::Barrier(b));
                }
                Message::Data(d) => {
                    if let Some(st) = state.as_mut() {
                        let result = mapper(d, &mut ctx.frontier, st);
                        output.send(BarrierData::Data(result));
                    }
                }
                Message::CeaseMarker(key) 
            }
        });
        self.then(operator)
    }
}
