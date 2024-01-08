use bincode::Encode;

use crate::{
    frontier::FrontierHandle,
    snapshot::{PersistenceBackend, barrier::BarrierData},
    stream::{
        jetstream::{Data, JetStreamBuilder},
        operator::StandardOperator,
    }, channels::selective_broadcast::Receiver,
};

pub trait StatefulMap<I, P: PersistenceBackend> {
    fn stateful_map<O: Data, S: 'static>(
        self,
        mapper: impl FnMut(I, &mut FrontierHandle, &mut S) -> O + 'static,
    ) -> JetStreamBuilder<O, P>;
}

impl<I, P> StatefulMap<I, P> for JetStreamBuilder<I, P>
where
    I: Data,
    P: PersistenceBackend,
{
    fn stateful_map<O: Data, S: 'static>(
        self,
        mut mapper: impl FnMut(I, &mut FrontierHandle, &mut S) -> O + 'static,
    ) -> JetStreamBuilder<O, P> {
        let mut state: Option<S> = None;
        let operator = StandardOperator::new(move |input: &mut Receiver<I, P>, output, frontier, operator_id: usize| {
            match input.recv() {
                Some(BarrierData::Load(l)) => {
                    let (time, loaded_state) = l.load(operator_id);
                    frontier.advance_to(time);
                    state = Some(loaded_state);
                }
                Some(BarrierData::Barrier(mut b)) => {
                    if let Some(st) = state.as_ref() {
                        b.persist(frontier.get_actual(), &st, operator_id);
                    }
                    output.send(BarrierData::Barrier(b));
                },
                Some(BarrierData::Data(d)) => {
                    if let Some(st) = state.as_mut() {
                        let result = mapper(d, frontier, st);
                        output.send(BarrierData::Data(result));
                    }
                },
                None => (),
            }
        });
        self.then(operator)
    }
}
