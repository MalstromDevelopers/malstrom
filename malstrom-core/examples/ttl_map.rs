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
    other: String,
}

/// Running total with TTL
fn build_running_total_dataflow(provider: &mut dyn StreamProvider) {
    let (ontime, _late) = provider
        .new_stream()
        .source(
            "source",
            StatelessSource::new(SingleIteratorSource::new(1..=25)),
        )
        .key_local("key-local", |x| ()) // only one key
        .assign_timestamps("assigner", |msg| msg.timestamp)
        .generate_epochs("generate", |msg, _| Some(msg.timestamp));

    ontime
        // sums up the numbers in blocks of 5
        .ttl_map(
            "running-total",
            async |_key, value, ts, mut state: TTLMyState| {
                match state.total.as_mut() {
                    // only update total keep same expiry
                    Some((total, _expiry)) => *total += value,
                    // let state expire in 5
                    None => state.set_total(value, ts + 5),
                }
                ((state.get_total().cloned(), value), Some(state))
            },
        )
        .sink("sink", StatelessSink::new(StdOutSink));
}
