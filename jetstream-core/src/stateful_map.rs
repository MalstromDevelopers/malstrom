use crate::{
    channels::selective_broadcast::Receiver,
    frontier::FrontierHandle,
    snapshot::{barrier::BarrierData, PersistenceBackend},
    stream::{
        jetstream::{Data, JetStreamBuilder},
        operator::StandardOperator,
    },
};

pub trait StatefulMap<I, P: PersistenceBackend> {
    fn stateful_map<O: Data, S: Default + 'static>(
        self,
        mapper: impl FnMut(I, &mut FrontierHandle, &mut S) -> O + 'static,
    ) -> JetStreamBuilder<O, P>;
}

impl<I, P> StatefulMap<I, P> for JetStreamBuilder<I, P>
where
    I: Data,
    P: PersistenceBackend,
{
    fn stateful_map<O: Data, S: Default + 'static>(
        self,
        mut mapper: impl FnMut(I, &mut FrontierHandle, &mut S) -> O + 'static,
    ) -> JetStreamBuilder<O, P> {
        let mut state: Option<S> = None;
        let operator = StandardOperator::new(move |input: &mut Receiver<I, P>, output, ctx| {
            match input.recv() {
                Some(BarrierData::Load(l)) => {
                    let (time, loaded_state) = l.load(ctx.operator_id).unwrap_or_default();
                    ctx.frontier.advance_to(time);
                    state = Some(loaded_state);
                }
                Some(BarrierData::Barrier(mut b)) => {
                    if let Some(st) = state.as_ref() {
                        b.persist(ctx.frontier.get_actual(), &st, ctx.operator_id);
                    }
                    output.send(BarrierData::Barrier(b));
                }
                Some(BarrierData::Data(d)) => {
                    if let Some(st) = state.as_mut() {
                        let result = mapper(d, &mut ctx.frontier, st);
                        output.send(BarrierData::Data(result));
                    }
                }
                None => (),
            }
        });
        self.then(operator)
    }
}
