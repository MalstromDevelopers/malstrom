use std::time::Duration;

/// A basic example which runs a no-op dataflow
use malstrom::runtime::{SingleThreadRuntime, SingleThreadRuntimeFlavor, StreamProvider, WorkerBuilder};
use malstrom::snapshot::{NoPersistence, NoSnapshots};

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
    provider.new_stream();
}
