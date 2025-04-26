//! A basic example which runs a no-op dataflow
use std::time::Duration;

use malstrom::runtime::SingleThreadRuntime;
use malstrom::snapshot::NoPersistence;
use malstrom::worker::StreamProvider;

fn main() {
    SingleThreadRuntime::builder()
        .snapshots(Duration::from_secs(300))
        .persistence(NoPersistence)
        .build(build_dataflow)
        .execute()
        .unwrap()
}

fn build_dataflow(provider: &mut dyn StreamProvider) -> () {
    provider.new_stream();
}
