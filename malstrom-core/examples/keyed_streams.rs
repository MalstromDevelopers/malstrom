use malstrom::keyed::partitioners::rendezvous_select;
use malstrom::operators::*;
/// A multithreaded, keyed stream
use malstrom::runtime::{MultiThreadRuntime, RuntimeFlavor, WorkerBuilder};
use malstrom::snapshot::{NoPersistence, NoSnapshots};
use malstrom::sources::{SingleIteratorSource, StatelessSource};

fn main() {
    MultiThreadRuntime::new(2, build_dataflow)
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
        .key_distribute("key-odd-even", |x| (x.value & 1) == 0, rendezvous_select)
        .inspect("print", |x, ctx| {
            println!("{x:?} @ Worker {}", ctx.worker_id)
        })
        .finish();
    worker
}
