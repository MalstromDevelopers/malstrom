use crate::frontier::Timestamp;
use crate::stream::jetstream::{Data, JetStreamBuilder};
use crate::stream::operator::StandardOperator;

pub trait Inspect<I> {
    fn inspect(self, inspector: impl FnMut(&I) -> () + 'static) -> JetStreamBuilder<I>;
}

impl<I> Inspect<I> for JetStreamBuilder<I>
where
    I: Data,
{
    fn inspect(self, mut inspector: impl FnMut(&I) -> () + 'static) -> JetStreamBuilder<I> {
        let operator = StandardOperator::new(move |input, output, frontier| {
            // since this operator does not participate in progress tracking
            // it must set u64::MAX to not block others from advancing
            let _ = frontier.advance_to(Timestamp::MAX);
            if let Some(msg) = input.recv() {
                inspector(&msg);
                output.send(msg);
            }
        });
        self.then(operator)
    }
}
