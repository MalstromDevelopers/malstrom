//! A basic example which runs a no-op dataflow
use malstrom::operators::*;
use malstrom::runtime::SingleThreadRuntime;
use malstrom::snapshot::NoPersistence;
use malstrom::sources::{SingleIteratorSource, StatelessSource};
use malstrom::worker::StreamProvider;

fn main() {
    SingleThreadRuntime::builder()
        .persistence(NoPersistence)
        .build(build_dataflow)
        .execute()
        .unwrap()
}

fn build_dataflow(provider: &mut dyn StreamProvider) -> () {
    let stream = provider
        .new_stream()
        .source(
            // this is an operator
            "iter-source",
            StatelessSource::new(SingleIteratorSource::new(0..=100)),
        )
        .map("double", async |x| x * 2)
        .inspect("print", async |x, _| println!("{}", x.value)); // <-- and this too
}
