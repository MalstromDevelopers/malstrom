use crate::{
    frontier::FrontierHandle,
    snapshot::{backend::{PersistentState, State}, barrier::BarrierData},
    stream::{
        jetstream::{Data, JetStreamBuilder},
        operator::StandardOperator,
    },
};

pub trait StatefulMap<I, P: PersistentState> {
    fn stateful_map<O: Data, S: State + 'static>(
        self,
        mapper: impl FnMut(I, &mut FrontierHandle, &mut S) -> O + 'static,
    ) -> JetStreamBuilder<O, P>;
}

impl<I, P> StatefulMap<I, P> for JetStreamBuilder<I, P>
where
    I: Data,
    P: PersistentState,
{
    fn stateful_map<O: Data, S: State>(
        self,
        mut mapper: impl FnMut(I, &mut FrontierHandle, &mut S) -> O + 'static,
    ) -> JetStreamBuilder<O, P> {
        let mut state: Option<S> = None;
        let operator = StandardOperator::new(move |input, output, frontier, persistence_backend: &mut P| {
            let st = state.get_or_insert_with(|| persistence_backend.load().unwrap_or_default());
            match input.recv() {
                Some(BarrierData::Barrier(b)) => {
                    persistence_backend.persist(frontier.get_actual(), st, b);
                    output.send(BarrierData::Barrier(b));
                },
                Some(BarrierData::Data(d)) => {
                    let result = mapper(d, frontier, st);
                    output.send(BarrierData::Data(result));
                },
                None => (),
            }
        });
        self.then(operator)
    }
}
