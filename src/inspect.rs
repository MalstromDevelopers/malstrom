use crate::frontier::Timestamp;
use crate::snapshot::backend::{NoState, PersistenceBackend};
use crate::stateful_map::StatefulMap;
use crate::stream::jetstream::{Data, JetStreamBuilder};

pub trait Inspect<I, P: PersistenceBackend> {
    fn inspect(self, inspector: impl FnMut(&I) -> () + 'static) -> JetStreamBuilder<I, P>;
}

impl<I, P> Inspect<I, P> for JetStreamBuilder<I, P>
where
    I: Data,
    P: PersistenceBackend,
{
    fn inspect(self, mut inspector: impl FnMut(&I) -> () + 'static) -> JetStreamBuilder<I, P> {
        self.stateful_map(move |input, frontier, NoState| {
            let _ = frontier.advance_to(Timestamp::MAX);
            inspector(&input);
            input
        })
    }
}
