use crate::{
    channels::selective_broadcast::{Receiver, Sender},
    frontier::FrontierHandle,
    snapshot::{
        backend::{PersistentState, State},
        barrier::BarrierData,
    },
    stream::{
        jetstream::{Data, JetStreamBuilder, NoData},
        operator::StandardOperator,
    },
};

pub trait StatefulSource<O: Data, P: PersistentState, S: State> {
    fn stateful_source(
        source_fn: impl FnMut(&mut FrontierHandle, &mut S) -> Option<O> + 'static,
    ) -> JetStreamBuilder<O, P>;
}

impl<O, P, S> StatefulSource<O, P, S> for JetStreamBuilder<NoData, P>
where
    O: Data,
    P: PersistentState,
    S: State,
{
    fn stateful_source(
        mut source_fn: impl FnMut(&mut FrontierHandle, &mut S) -> Option<O> + 'static,
    ) -> JetStreamBuilder<O, P> {
        let mut state: Option<S> = None;
        let operator = StandardOperator::new(
            move |input: &mut Receiver<NoData>,
                  output: &mut Sender<O>,
                  frontier: &mut FrontierHandle,
                  p_state: &mut P| {
                let st = state.get_or_insert_with(|| p_state.load().unwrap_or_default());
                match input.recv() {
                    Some(BarrierData::Barrier(b)) => {
                        p_state.persist(frontier.get_actual(), st, b);
                        output.send(BarrierData::Barrier(b))
                    }
                    _ => {
                        if let Some(x) = source_fn(frontier, st) {
                            output.send(BarrierData::Data(x))
                        }
                    }
                }
            },
        );
        JetStreamBuilder::from_operator(operator)
    }
}

pub trait PollSource<O: Data, P: PersistentState> {
    fn poll_source(
        source_fn: impl FnMut(&mut FrontierHandle) -> Option<O> + 'static,
    ) -> JetStreamBuilder<O, P>;
}

impl<O, P> PollSource<O, P> for JetStreamBuilder<NoData, P>
where
    O: Data,
    P: PersistentState,
{
    fn poll_source(
        mut source_fn: impl FnMut(&mut FrontierHandle) -> Option<O> + 'static,
    ) -> JetStreamBuilder<O, P> {
        JetStreamBuilder::stateful_source(move |f, _state: &mut ()| source_fn(f))
    }
}
