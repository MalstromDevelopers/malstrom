use crate::{
    state_backend::StateBackend,
    stream::{dist_rand, Data, JetStream, StandardOperator},
};

use std::hash::Hash;

pub trait StatefulMap<I> {
    fn stateful_map<O: Data, S, K, V>(
        self,
        mapper: impl FnMut(I, &mut S) -> O + 'static,
        state_backend: S,
    ) -> JetStream<O>
    where
        S: StateBackend<K, V> + 'static,
        K: PartialEq + Eq + Hash,
        V: Data;
}

impl<O> StatefulMap<O> for JetStream<O>
where
    O: Data,
{
    fn stateful_map<T: Data, S, K, V>(
        self,
        mut mapper: impl FnMut(O, &mut S) -> T + 'static,
        mut state_backend: S,
    ) -> JetStream<T>
    where
        S: StateBackend<K, V> + 'static,
        K: PartialEq + Eq + Hash,
        V: Data,
    {
        let operator = StandardOperator::new(
            move |inputs: &Vec<crossbeam::channel::Receiver<O>>, outputs, _| {
                let inp = inputs.iter().filter_map(|x| x.try_recv().ok());
                let mut data = Vec::new();
                for x in inp {
                    // this is super weird, but I could not get the code to borrow
                    // check otherwise
                    data.push(mapper(x, &mut state_backend))
                }
                dist_rand(data.into_iter(), outputs)
            },
        );
        self.then(operator)
    }
}

