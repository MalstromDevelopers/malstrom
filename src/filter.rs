use crate::stream::{dist_rand, Data, JetStream, StandardOperator};

pub trait Filter<O> {
    fn filter(self, filter: impl FnMut(&O) -> bool + 'static) -> JetStream<O>;
}

impl<O> Filter<O> for JetStream<O>
where
    O: Data,
{
    fn filter(self, mut filter: impl FnMut(&O) -> bool + 'static) -> JetStream<O> {
        let operator = StandardOperator::new(move |inputs, outputs, _| {
            let inp = inputs.iter().filter_map(|x| x.try_recv().ok());
            let mut data = Vec::new();
            for x in inp {
                // this is super weird, but I could not get the code to borrow
                // check otherwise
                if filter(&x) {
                    data.push(x)
                }
            }
            dist_rand(data.into_iter(), outputs)
        });
        self.then(operator)
    }
}
