use std::time::Duration;

use malstrom::operators::*;
/// A basic example which runs a no-op dataflow
use malstrom::runtime::{
    SingleThreadRuntime, SingleThreadRuntimeFlavor, StreamProvider, WorkerBuilder,
};
use malstrom::snapshot::{NoPersistence, NoSnapshots};
use malstrom::sources::{SingleIteratorSource, StatelessSource};

fn main() {
    tracing_subscriber::fmt::init();
    SingleThreadRuntime::builder()
        .snapshots(Duration::from_secs(300))
        .persistence(NoPersistence::default())
        .build(build_dataflow)
        .execute()
        .unwrap()
}

fn build_dataflow(provider: &mut dyn StreamProvider) -> () {
    provider
        .new_stream()
        .source(
            // <-- this is an operator
            "iter-source",
            StatelessSource::new(SingleIteratorSource::new(0..=100)),
        )
        .map("double", |x| x * 2)
        .inspect("print", |x, _| println!("{}", x.value)); // <-- and this too
}
