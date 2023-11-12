use crate::stream::{
    jetstream::{Data, JetStreamBuilder},
    operator::StandardOperator,
};

pub trait StatefulMap<I> {
    fn stateful_map<O: Data, S: 'static>(
        self,
        mapper: impl FnMut(I, &mut S) -> O + 'static,
        state_backend: S,
    ) -> JetStreamBuilder<O>;
}

impl<O> StatefulMap<O> for JetStreamBuilder<O>
where
    O: Data,
{
    fn stateful_map<T: Data, S: 'static>(
        self,
        mut mapper: impl FnMut(O, &mut S) -> T + 'static,
        mut state_backend: S,
    ) -> JetStreamBuilder<T> {
        let operator = StandardOperator::new(move |input, output, frontier| {
            // since this operator does not participate in progress tracking
            // it must set u64::MAX to not block others from advancing
            // TODO: Should the stateful map participate?
            let _ = frontier.advance_to(u64::MAX);
            if let Some(msg) = input.recv() {
                output.send(mapper(msg, &mut state_backend));
            }
        });
        self.then(operator)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::{frontier::FrontierHandle, worker::Worker, stream::jetstream::JetStreamEmpty, map::Map};

    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn test_stateful_map() {
        let mut worker = Worker::new();
        let state: HashMap<u8, i32> = HashMap::new();

        let source = |frontier_handle: &mut FrontierHandle| {
            frontier_handle.advance_by(1).unwrap();
            Some(1)
        };
        let stream = JetStreamEmpty::new()
            .source(source)
            .stateful_map(
                |x, state| {
                    let new_val = state.get(&0).unwrap_or(&0) + x;
                    state.insert(0, new_val);
                    new_val
                },
                state,
            )
            .map(|o| {
                assert_eq!(o, 1);})
            .build();
        worker.add_stream(stream);
        worker.step(); // source 
        worker.step(); // stateful map
        worker.step(); // map
    }
}
