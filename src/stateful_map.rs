use crate::{
    frontier::FrontierHandle,
    snapshot::backend::{PersistenceBackend, State},
    stream::{
        jetstream::{Data, JetStreamBuilder},
        operator::StandardOperator,
    },
};

pub trait StatefulMap<I, P: PersistenceBackend> {
    fn stateful_map<O: Data, S: State + 'static>(
        self,
        mapper: impl FnMut(I, &mut FrontierHandle, &mut S) -> O + 'static,
    ) -> JetStreamBuilder<O, P>;
}

impl<I, P> StatefulMap<I, P> for JetStreamBuilder<I, P>
where
    I: Data,
    P: PersistenceBackend,
{
    fn stateful_map<O: Data, S: State>(
        self,
        mut mapper: impl FnMut(I, &mut FrontierHandle, &mut S) -> O + 'static,
    ) -> JetStreamBuilder<O, P> {
        let operator = StandardOperator::new(move |input, frontier, state| {
            if let Some(x) = input {
                Some(mapper(x, frontier, state))
            } else {
                None
            }
        });
        self.then(operator)
    }
}
