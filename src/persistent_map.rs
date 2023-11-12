use crate::{
    state_backend::{PersistentState, PersistentStateBackend},
    stream::{
        jetstream::{Data, JetStreamBuilder},
        operator::StandardOperator,
    },
};

pub trait StatefulMap<I> {
    /// Behaves like a stateful map but will use the persistent backend
    /// to load the value before executing the map function
    /// and saves the value to the persistent backend after the execution
    ///
    /// # Arguments
    /// mapper: Mapping function which takes in the stream value and
    ///     the state
    /// state_backend: persistent backend implementing save and persists
    ///     functionality
    ///
    /// # Note
    /// If you want to modify the load and persist behaviour, use a
    /// standard stateful map and implement the call to `load` and
    /// `persist` manually in your mapper function.
    fn persistent_map<O: Data, S: PersistentState + 'static>(
        self,
        mapper: impl FnMut(I, &mut S) -> O + 'static,
        state_backend: impl PersistentStateBackend<S> + 'static,
    ) -> JetStreamBuilder<O>;
}

impl<O> StatefulMap<O> for JetStreamBuilder<O>
where
    O: Data,
{
    fn persistent_map<T: Data, S: PersistentState + 'static>(
        self,
        mut mapper: impl FnMut(O, &mut S) -> T + 'static,
        state_backend: impl PersistentStateBackend<S> + 'static,
    ) -> JetStreamBuilder<T> {
        let operator = StandardOperator::new(move |input, output, frontier| {
            // since this operator does not participate in progress tracking
            // it must set u64::MAX to not block others from advancing
            // TODO: Should the stateful map participate?
            let _ = frontier.advance_to(u64::MAX);

            let mut state = state_backend.load().unwrap_or_default();

            if let Some(msg) = input.recv() {
                output.send(mapper(msg, &mut state));
            }

            // persist state after it has been mutated
            state_backend.persist(state);
        });
        self.then(operator)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        frontier::FrontierHandle, map::Map, state_backend::FilesystemStateBackend,
        stream::jetstream::JetStreamEmpty, worker::Worker,
    };

    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;
    use bincode::config;
    use pretty_assertions::assert_eq;
    use tempfile::NamedTempFile;

    #[test]
    fn test_persistent_map() {
        let path = NamedTempFile::new()
            .expect("Could not create temp file")
            .into_temp_path()
            .to_str()
            .unwrap()
            .to_string();

        let mut worker = Worker::new();
        let state_backend: FilesystemStateBackend<i32> =
            FilesystemStateBackend::new(path, config::standard());

        let source = |frontier_handle: &mut FrontierHandle| {
            frontier_handle.advance_by(1).unwrap();
            Some(1)
        };
        let stream = JetStreamEmpty::new()
            .source(source)
            .persistent_map(
                |x, state| {
                    *state += x;

                    *state
                },
                state_backend,
            )
            .map(|o| {
                println!("{o}");
                assert_eq!(o, 1);
            })
            .build();
        worker.add_stream(stream);
        worker.step(); // source
        worker.step(); // stateful map
        worker.step(); // map
    }
}
