use crate::stream::{dist_rand, Data, JetStream, StandardOperator};

pub trait StatefulMap<I> {
    fn stateful_map<O: Data, S: 'static>(
        self,
        mapper: impl FnMut(I, &mut S) -> O + 'static,
        state_backend: S,
    ) -> JetStream<O>;
}

impl<O> StatefulMap<O> for JetStream<O>
where
    O: Data,
{
    fn stateful_map<T: Data, S: 'static>(
        self,
        mut mapper: impl FnMut(O, &mut S) -> T + 'static,
        mut state_backend: S,
    ) -> JetStream<T>
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::{frontier::FrontierHandle, worker::Worker};

    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn test_stateful_map() {
        let mut worker = Worker::new();
        let state: HashMap<u8, f64> = HashMap::new();

        let source = |frontier_handle: &mut FrontierHandle| {
            frontier_handle.advance_by(1).unwrap();
            Some(rand::random::<f64>())
        };
        let stream = JetStream::new()
            .source(source)
            .stateful_map(
                |x, state| {
                    let new_val = state.get(&0).unwrap_or(&0.0)+ x;
                    state.insert(0, new_val);
                    x
                },
                state,
            )
            .finalize();
        let probe = stream.get_probe();
        worker.add_stream(stream);
        assert_eq!(probe.read(), 0);
        worker.step();
        assert_eq!(probe.read(), 1);

        assert_eq!(worker.get_frontier().unwrap(), 1);
    }

    #[test]
    fn test_persistent_map() {
        let mut worker = Worker::new();
        let state: HashMap<u8, f64> = HashMap::new();

        let source = |frontier_handle: &mut FrontierHandle| {
            frontier_handle.advance_by(1).unwrap();
            Some(rand::random::<f64>())
        };
        let stream = JetStream::new()
            .source(source)
            .stateful_map(
                |x, state| {
                    let new_val = state.get(&0).unwrap_or(&0.0)+ x;
                    state.insert(0, new_val);
                    x
                },
                state,
            )
            .finalize();
        let probe = stream.get_probe();
        worker.add_stream(stream);
        assert_eq!(probe.read(), 0);
        worker.step();
        assert_eq!(probe.read(), 1);

        assert_eq!(worker.get_frontier().unwrap(), 1);
    }
}
