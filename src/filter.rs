use crate::frontier::Timestamp;
use crate::stream::jetstream::{Data, JetStreamBuilder};
use crate::stream::operator::StandardOperator;

pub trait Filter<O> {
    fn filter(self, filter: impl FnMut(&O) -> bool + 'static) -> JetStreamBuilder<O>;
}

impl<O> Filter<O> for JetStreamBuilder<O>
where
    O: Data,
{
    fn filter(self, mut filter: impl FnMut(&O) -> bool + 'static) -> JetStreamBuilder<O> {
        let operator = StandardOperator::new(move |input, output, frontier| {
            // since this operator does not participate in progress tracking
            // it must set u64::MAX to not block others from advancing
            let _ = frontier.advance_to(Timestamp::MAX);
            if let Some(msg) = input.recv() {
                if filter(&msg) {
                    output.send(msg)
                }
            }
        });
        self.then(operator)
    }
}
