use std::sync::Arc;
use std::sync::RwLock;

use serde::de::DeserializeOwned;
use serde::Serialize;
use timely::dataflow::operators::Map;
use timely::dataflow::stream::Stream;
use timely::dataflow::InputHandle;
use timely::dataflow::Scope;
use timely::dataflow::ScopeParent;
use timely::Data;

pub trait PersistantState: Data + Serialize + DeserializeOwned {}
impl<T: Data + Serialize + DeserializeOwned> PersistantState for T {}

fn load_state<P: DeserializeOwned>() -> Option<P> {
    let state_file = std::path::Path::new("state.bin");
    println!("Loading state");
    let fileinp = std::fs::read(state_file).ok()?;
    let state: P = bincode::deserialize(&fileinp).ok()?;
    Some(state)
}

fn save_state<P: Serialize>(state: &P) -> Option<()> {
    let state_file = std::path::Path::new("state.bin");

    println!("Saving state");
    // TODO: make a result
    let bytes = bincode::serialize(state).unwrap();
    std::fs::write(state_file, bytes).unwrap();
    Some(())
}

/// Create a new stream from a persisted stateful source
/// 
/// This works by supplying a timely worker's scope
/// 
/// A factory to create the initial state (if it is not on disk)
/// A source which generates records
pub fn stateful_source<P: PersistantState, S: Scope, D: Data>(
    scope: &mut S,
    factory: impl Fn() -> P,
    source: impl Fn(P) -> (D, P) + 'static,
) -> Stream<S, D>
where
    S: ScopeParent<Timestamp = usize>,
{
    // TODO we hardcode the timestamp type to usize here
    // but that may no be desirable to users
    let state_input = Arc::new(RwLock::new(InputHandle::<usize, P>::new()));
    let state_input_inner = state_input.clone();

    let state_stream = state_input.write().unwrap().to_stream(scope);
    // call the given function to generate input then
    // disentangle the state and data. Feed the state into the state_input
    // and the data downstream
    let data_stream = state_stream
        .map(source)
        .map_in_place(|(_, state)| {
            save_state(&state);
        })
        // destructure
        .map(move |(data, state)| {
            // feed the state back into the input
            let mut state_input_handle = state_input_inner.write().unwrap();
            let time = state_input_handle.time().clone();
            state_input_handle.send(state);
            state_input_handle.advance_to(time + 1);
            data
        });

    // load or create the initial state
    let initial_state = load_state().unwrap_or_else(factory);
    state_input.write().unwrap().send(initial_state);
    state_input.write().unwrap().advance_to(1);
    data_stream
}

#[cfg(test)]
mod tests {
    use timely::dataflow::operators::Inspect;

    use super::*;

    #[test]
    fn it_works() {
        let factory = || {
            0 as usize
        };
        let logic = |state| {
            if state < 20 {
                println!("Source called");
                return (4, state + 1);
            } else {
                std::process::exit(0)
            }
        };

        timely::execute_from_args(std::env::args(), move |worker| {
            worker.dataflow::<usize, _, _>(|scope| {
                stateful_source(scope, factory, logic).inspect(|x| println!("{x:?}"));
            });
            for _ in 0..10 {
                worker.step();
            }
        })
        .unwrap();
    }
}
