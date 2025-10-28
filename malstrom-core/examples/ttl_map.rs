//! Usage example for the ttl_map operator
use expiremap::ExpireMap;
use malstrom::keyed::KeyLocal;
use malstrom::operators::*;
use malstrom::runtime::SingleThreadRuntime;
use malstrom::sinks::{StatelessSink, StdOutSink};
use malstrom::snapshot::NoPersistence;
use malstrom::sources::{SingleIteratorSource, StatelessSource};
use malstrom::worker::StreamProvider;
use std::time::Duration;

fn main() {
    SingleThreadRuntime::builder()
        .snapshots(Duration::from_secs(300))
        .persistence(NoPersistence)
        .build(build_running_total_dataflow)
        .execute()
        .unwrap();
}

#[derive(TTLState)] // this generates the type TTLMyState
#[timestamp_type(usize)]
struct MyState {
    total: i32,
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
            async |_key, value, ts, mut state: TTLMyState| {
                match state.get_total() {
                    Some(total) => state.set_total(total + value, ts + 15),
                    None => state.set_total(value, ts + 15),
                }
                (value, Some(state))
            },
        )
        .sink("sink", StatelessSink::new(StdOutSink));
}
