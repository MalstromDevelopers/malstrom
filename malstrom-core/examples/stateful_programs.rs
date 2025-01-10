use malstrom::keyed::partitioners::rendezvous_select;
use malstrom::operators::*;
/// A stateful program
use malstrom::runtime::{RuntimeFlavor, SingleThreadRuntime, WorkerBuilder};
use malstrom::snapshot::{NoPersistence, NoSnapshots};
use malstrom::sources::{SingleIteratorSource, StatelessSource};

fn main() {
    SingleThreadRuntime::new(build_dataflow).execute().unwrap()
}

fn build_dataflow<F: RuntimeFlavor>(flavor: F) -> WorkerBuilder<F, NoPersistence> {
    let mut worker = WorkerBuilder::new(flavor, NoSnapshots, NoPersistence::default());
    worker
        .new_stream()
        .source(
            "iter-source",
            StatelessSource::new(SingleIteratorSource::new(0..=100)),
        )
        .key_distribute("key-by-value", |_| 0, rendezvous_select)
        .stateful_map("sum", |_key, value, state: i32| {
            let state = state + value;
            (state.clone(), Some(state))
        })
        .inspect("print", |x, _ctx_| println!("{}", x.value))
        .finish();
    worker
}
