use crossbeam::channel::{Receiver, Sender};

use crate::poc::{dist_rand, Data, JetStream, StandardOperator};

pub trait Filter<O> {
    fn filter(self, filter: impl FnMut(&O) -> bool + 'static) -> JetStream<O, O>;
}

impl<I, O> Filter<O> for JetStream<I, O>
where
    I: Data,
    O: Data,
{
    fn filter(self, mut filter: impl FnMut(&O) -> bool + 'static) -> JetStream<O, O> {
        let operator = StandardOperator::new(move |inputs, outputs| {
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
