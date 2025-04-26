//! Example using stateful_map with multiple keys
use malstrom::keyed::partitioners::rendezvous_select;
use malstrom::operators::*;
use malstrom::runtime::SingleThreadRuntime;
use malstrom::sinks::{StatelessSink, StdOutSink};
use malstrom::snapshot::NoPersistence;
use malstrom::sources::{SingleIteratorSource, StatelessSource};
use malstrom::worker::StreamProvider;

fn main() {
	SingleThreadRuntime::builder().persistence(NoPersistence).build(build_dataflow).execute().unwrap();
}

fn build_dataflow(provider: &mut dyn StreamProvider) {
	provider
		.new_stream()
		.source(
			"iter-source",
			StatelessSource::new(SingleIteratorSource::new(0..=100))
		)
		.key_distribute("key-by-value", |x| x.value & 1 == 1, rendezvous_select)
		.stateful_map("sum", |_key, value, state| {
			let state: i32 = state + value;
			(state, Some(state))
		})
        .sink("stdout", StatelessSink::new(StdOutSink));
}
