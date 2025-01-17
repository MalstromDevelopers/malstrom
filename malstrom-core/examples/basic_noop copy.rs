use std::time::Duration;

/// A basic example which runs a no-op dataflow
use malstrom::runtime::{SingleThreadRuntime, SingleThreadRuntimeFlavor, WorkerBuilder};
use malstrom::snapshot::{NoPersistence, NoSnapshots};

fn main() {
    SingleThreadRuntime::new()
    .with_snapshots(Duration::from_secs(300))
    .with_persistence(NoPersistence::default())
    .build(build_dataflow)
    .execute()
    .unwrap()
}

fn build_dataflow(provider: &mut dyn StreamProvider) -> () {
    provider.new_stream().finish();
}
