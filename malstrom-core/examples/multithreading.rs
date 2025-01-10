use malstrom::keyed::partitioners::rendezvous_select;
use malstrom::operators::*;
/// A multithreaded program
use malstrom::runtime::{MultiThreadRuntime, RuntimeFlavor, WorkerBuilder};
use malstrom::snapshot::{NoPersistence, NoSnapshots};
use malstrom::sources::{SingleIteratorSource, StatelessSource};

fn main() {
    MultiThreadRuntime::new(4, build_dataflow)
        .execute()
        .unwrap()
}

fn build_dataflow<F: RuntimeFlavor>(flavor: F) -> WorkerBuilder<F, NoPersistence> {
    let mut worker = WorkerBuilder::new(flavor, NoSnapshots, NoPersistence::default());
    worker
        .new_stream()
        .source(
            "iter-source",
            StatelessSource::new(SingleIteratorSource::new(0..=100)),
        )
        .key_distribute("key-by-value", |x| x.value, rendezvous_select)
        .map("double", |x| x * 2)
        .inspect("print", |x, ctx| {
            println!("{x:?} @ Worker {}", ctx.worker_id)
        })
        .finish();
    worker
}
