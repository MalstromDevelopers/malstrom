//! Using SlateDB as a persistence backend
use malstrom::keyed::partitioners::rendezvous_select;
use malstrom::operators::*;
use malstrom::sinks::{StatelessSink, StdOutSink};
use malstrom::{
    runtime::SingleThreadRuntime,
    snapshot::SlateDbBackend,
    sources::{SingleIteratorSource, StatelessSource},
    worker::StreamProvider,
};
use object_store::{local::LocalFileSystem, path::Path};
use std::sync::Arc;
use std::time::Duration;

fn main() {
    let filesystem = LocalFileSystem::new();
    let persistence = SlateDbBackend::new(Arc::new(filesystem), Path::from("/tmp")).unwrap();

    SingleThreadRuntime::builder()
        .persistence(persistence)
        .snapshots(Duration::from_secs(10))
        .build(build_dataflow)
        .execute()
        .unwrap()
}

fn build_dataflow(provider: &mut dyn StreamProvider) {
    provider
        .new_stream()
        .source(
            "iter-source",
            StatelessSource::new(SingleIteratorSource::new(0..=100)),
        )
        .key_distribute("key-by-value", |x| x.value & 1 == 1, rendezvous_select)
        .stateful_map("sum", |_key, value, state: i32| {
            let state = state + value;
            (state, Some(state))
        })
        .sink("stdout", StatelessSink::new(StdOutSink));
}
