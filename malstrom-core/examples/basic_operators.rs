//! A basic example which runs a no-op dataflow
use malstrom::operators::*;
use malstrom::runtime::SingleThreadRuntime;
use malstrom::snapshot::NoPersistence;
use malstrom::sources::{SingleIteratorSource, StatelessSource};
use malstrom::worker::StreamProvider;

fn main() {
    tracing_subscriber::fmt::init();
    SingleThreadRuntime::builder()
        .persistence(NoPersistence)
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
