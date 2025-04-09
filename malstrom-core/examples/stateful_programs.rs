use std::time::Duration;

use malstrom::keyed::partitioners::rendezvous_select;
use malstrom::operators::*;
/// A stateful program
use malstrom::runtime::{SingleThreadRuntime, StreamProvider};
use malstrom::snapshot::NoPersistence;
use malstrom::sources::{SingleIteratorSource, StatelessSource};

fn main() {
    SingleThreadRuntime::builder()
        .snapshots(Duration::from_secs(300))
        .persistence(NoPersistence)
        .build(build_dataflow)
        .execute()
        .unwrap()
}

fn build_dataflow(provider: &mut dyn StreamProvider) -> () {
    provider
        .new_stream()
        .source(
            "iter-source",
            StatelessSource::new(SingleIteratorSource::new(0..=100)),
        )
        .key_distribute("key-by-value", |_| 0, rendezvous_select)
        .stateful_map("sum", |_key, value, state: i32| {
            let state = state + value;
            (state.clone(), Some(state))
        })
        .inspect("print", |x, _ctx_| println!("{}", x.value));
}
