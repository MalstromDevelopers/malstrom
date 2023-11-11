use crate::stream::jetstream::{Data, JetStreamBuilder};
use crate::stream::operator::StandardOperator;

pub trait Map<I> {
    fn map<O: Data>(self, mapper: impl FnMut(I) -> O + 'static) -> JetStreamBuilder<O>;
}

impl<O> Map<O> for JetStreamBuilder<O>
where
    O: Data,
{
    fn map<T: Data>(self, mut mapper: impl FnMut(O) -> T + 'static) -> JetStreamBuilder<T> {
        let operator = StandardOperator::new(move |input, output, frontier| {
            // since this operator does not participate in progress tracking
            // it must set u64::MAX to not block others from advancing
            let _ = frontier.advance_to(u64::MAX);
            if let Some(msg) = input.recv() {
                output.send(mapper(msg))
            }
        });
        self.then(operator)
    }
}
