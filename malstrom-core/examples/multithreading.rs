use std::time::Duration;

use malstrom::keyed::partitioners::rendezvous_select;
use malstrom::operators::*;
/// A multithreaded program
use malstrom::runtime::{MultiThreadRuntime, RuntimeFlavor, StreamProvider, WorkerBuilder};
use malstrom::snapshot::{NoPersistence, NoSnapshots};
use malstrom::sources::{SingleIteratorSource, StatelessSource};

fn main() {
    tracing_subscriber::fmt::init();
    MultiThreadRuntime::builder()
        .parrallelism(4)
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
            StatelessSource::new(SingleIteratorSource::new((0..=100))),
        )
        .key_distribute("key-by-value", |x| x.value, rendezvous_select)
        .map("double", |x| x * 2)
        .inspect("print", |x, ctx| {
            println!("{x:?} @ Worker {}", ctx.worker_id)
        });
}
