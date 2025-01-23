use malstrom::keyed::partitioners::rendezvous_select;
use malstrom::operators::*;
/// A multithreaded, keyed stream
use malstrom::runtime::{MultiThreadRuntime, RuntimeFlavor, StreamProvider, WorkerBuilder};
use malstrom::snapshot::{NoPersistence, NoSnapshots};
use malstrom::sources::{SingleIteratorSource, StatelessSource};

fn main() {
    MultiThreadRuntime::builder()
        .parrallelism(2)
        .persistence(NoPersistence::default())
        .build(build_dataflow)
        .execute()
        .unwrap()
}

fn build_dataflow(provider: &mut dyn StreamProvider) -> () {
    provider
        .new_stream()
        .source(
            "iter-source",
            StatelessSource::new(SingleIteratorSource::new(0..=100)),
        )
        .key_distribute("key-odd-even", |x| (x.value & 1) == 0, rendezvous_select)
        .inspect("print", |x, ctx| {
            println!("{x:?} @ Worker {}", ctx.worker_id)
        });
}
