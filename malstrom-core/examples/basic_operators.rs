/// A very basic example which uses stream operators
use malstrom::operators::*;
use malstrom::runtime::{SingleThreadRuntime, SingleThreadRuntimeFlavor, WorkerBuilder};
use malstrom::snapshot::{NoPersistence, NoSnapshots};
use malstrom::sources::{SingleIteratorSource, StatelessSource};

fn main() {
    SingleThreadRuntime::new(build_dataflow).execute().unwrap()
}

fn build_dataflow(
    flavor: SingleThreadRuntimeFlavor,
) -> WorkerBuilder<SingleThreadRuntimeFlavor, NoPersistence> {
    let mut worker = WorkerBuilder::new(flavor, NoSnapshots, NoPersistence::default());
    worker
        .new_stream()
        .source(
            // <-- this is an operator
            "iter-source",
            StatelessSource::new(SingleIteratorSource::new(0..=100)),
        )
        .map("double", |x| x * 2)
        .inspect("print", |x, _| println!("{}", x.value)) // <-- and this too
        .finish();
    worker
}
