//! Combining multiple streams
use malstrom::operators::*;
use malstrom::runtime::SingleThreadRuntime;
use malstrom::sinks::{StatelessSink, StdOutSink};
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
    let numbers = provider.new_stream().source(
        "iter-source",
        StatelessSource::new(SingleIteratorSource::new(0..=100)),
    );
    let more_numbers = provider.new_stream().source(
        "other-iter-source",
        StatelessSource::new(SingleIteratorSource::new(0..=100)),
    );

    numbers
        .union([more_numbers].into_iter())
        .sink("std-out-sink", StatelessSink::new(StdOutSink));
}
