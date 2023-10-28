use crossbeam::channel::{Receiver, Sender};

use crate::poc::{dist_rand, Data, JetStream, StandardOperator};

pub trait Map<I> {
    fn map<O: Data>(self, mapper: impl FnMut(I) -> O + 'static) -> JetStream<I, O>;
}

impl<I, O> Map<O> for JetStream<I, O>
where
    I: Data,
    O: Data,
{
    fn map<T: Data>(self, mut mapper: impl FnMut(O) -> T + 'static) -> JetStream<O, T> {
        let operator = StandardOperator::new(move |inputs, outputs| {
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
// pub fn source(
//     self,
//     mut source_func: impl FnMut() -> Option<O> + 'static,
// ) -> JetStream<Nothing, O> {
//     let last_op = StandardOperator::new(move |_inputs, outputs| {
//         match source_func() {
//             Some(x) => dist_rand(vec![x], outputs),
//             None => {}
//         }
//     });