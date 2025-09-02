//! Usage example for the ttl_map operator
use malstrom::operators::*;
use malstrom::runtime::SingleThreadRuntime;
use malstrom::sinks::{StatelessSink, StdOutSink};
use malstrom::sources::{SingleIteratorSource, StatelessSource};
use malstrom::snapshot::NoPersistence;
use malstrom::worker::StreamProvider;
use malstrom::keyed::KeyLocal;
use expiremap::ExpireMap;
use std::time::Duration;

fn main() {
    SingleThreadRuntime::builder()
        .snapshots(Duration::from_secs(300))
        .persistence(NoPersistence)
        .build(build_running_total_dataflow)
        .execute()
        .unwrap();
}

/// Running total with TTL
fn build_running_total_dataflow(provider: &mut dyn StreamProvider) {
    let (ontime, _late) = provider
        .new_stream()
        .source(
            "source",
            StatelessSource::new(SingleIteratorSource::new(0..100)),
        )
        .key_local("key-local", |x| (x.value & 1) == 1) // Group by odd/even
        .assign_timestamps("assigner", |msg| msg.timestamp)
        .generate_epochs("generate", |_, t| t.to_owned());

    ontime
        .ttl_map(
            "running-total",
            |_key, inp, ts, mut state: ExpireMap<String, i32, usize>| {
                let g = state.get(&"total".to_owned());
                let val = if let Some(val) = g {
                    let v = inp + *val;
                    state.insert("total".to_owned(), v, ts + 15);
                    v
                } else {
                    state.insert("total".to_owned(), inp, ts + 15);
                    inp
                };
                (val, Some(state))
            },
        )
        .sink("sink", StatelessSink::new(StdOutSink));
}
