use crate::stream::jetstream::{dist_rand, Data, JetStream, JetStreamBuilder};
use crate::stream::operator::OperatorBuilder;

pub trait Map<I> {
    fn map<O: Data>(self, mapper: impl FnMut(I) -> O + 'static) -> JetStreamBuilder<O>;
}

impl<O> Map<O> for JetStreamBuilder<O>
where
    O: Data,
{
    fn map<T: Data>(self, mut mapper: impl FnMut(O) -> T + 'static) -> JetStreamBuilder<T> {
        let operator = OperatorBuilder::new(move |inputs, outputs, _| {
            let inp = inputs.iter().filter_map(|x| x.try_recv().ok());
            let mut data = Vec::new();
            for x in inp {
                // this is super weird, but I could not get the code to borrow
                // check otherwise
                data.push(mapper(x))
            }
            dist_rand(data.into_iter(), outputs)
        });
        self.then(operator)
    }
}
