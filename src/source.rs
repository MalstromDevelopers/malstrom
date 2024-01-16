use crate::{
    channels::selective_broadcast::{Receiver, Sender},
    frontier::FrontierHandle,
    snapshot::{barrier::BarrierData, PersistenceBackend},
    stream::{
        jetstream::{Data, JetStreamBuilder, NoData},
        operator::StandardOperator,
    },
};

pub trait StatefulSource<O: Data, P: PersistenceBackend, S> {
    fn stateful_source(
        self,
        source_fn: impl FnMut(&mut FrontierHandle, &mut S) -> Option<O> + 'static,
    ) -> JetStreamBuilder<O, P>;
}

impl<O, P, S> StatefulSource<O, P, S> for JetStreamBuilder<NoData, P>
where
    O: Data,
    P: PersistenceBackend,
    S: Default + 'static,
{
    fn stateful_source(
        self,
        mut source_fn: impl FnMut(&mut FrontierHandle, &mut S) -> Option<O> + 'static,
    ) -> JetStreamBuilder<O, P> {
        let mut state: Option<S> = None;
        let operator = StandardOperator::new(
            move |input: &mut Receiver<NoData, P>,
                  output: &mut Sender<O, P>,
                  frontier: &mut FrontierHandle,
                  operator_id: usize| {
                match input.recv() {
                    Some(BarrierData::Load(l)) => {
                        let (time, loaded_state) = l.load(operator_id).unwrap_or_default();
                        frontier.advance_to(time);
                        state = Some(loaded_state);
                        output.send(BarrierData::Load(l))
                    }
                    Some(BarrierData::Barrier(mut b)) => {
                        if let Some(st) = state.as_ref() {
                            b.persist(frontier.get_actual(), st, operator_id)
                        }
                        output.send(BarrierData::Barrier(b))
                    }
                    _ => {
                        if let Some(st) = state.as_mut() {
                            if let Some(x) = source_fn(frontier, st) {
                                output.send(BarrierData::Data(x))
                            }
                        }
                    }
                }
            },
        );
        self.then(operator)
    }
}

pub trait PollSource<O: Data, P: PersistenceBackend> {
    fn poll_source(
        self,
        source_fn: impl FnMut(&mut FrontierHandle) -> Option<O> + 'static,
    ) -> JetStreamBuilder<O, P>;
}

impl<O, P> PollSource<O, P> for JetStreamBuilder<NoData, P>
where
    O: Data,
    P: PersistenceBackend,
{
    fn poll_source(
        self,
        mut source_fn: impl FnMut(&mut FrontierHandle) -> Option<O> + 'static,
    ) -> JetStreamBuilder<O, P> {
        self.stateful_source(move |f, _state: &mut ()| source_fn(f))
    }
}
