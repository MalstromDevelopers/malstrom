// main.rs
use malstrom::operators::*;
use malstrom::runtime::{
    RuntimeFlavor, SingleThreadRuntime, WorkerBuilder,
};
use malstrom::sinks::{StatelessSink, StdOutSink};
use malstrom::snapshot::{NoPersistence, NoSnapshots};
use malstrom::sources::{SingleIteratorSource, StatelessSource};

fn main() {
    SingleThreadRuntime::new(build_dataflow).execute().unwrap()
}

fn build_dataflow<F: RuntimeFlavor>(flavor: F) -> WorkerBuilder<F, NoPersistence> {
    let mut worker = WorkerBuilder::new(flavor, NoSnapshots, NoPersistence::default());
    let numbers = worker.new_stream().source(
        "iter-source",
        StatelessSource::new(SingleIteratorSource::new(0..=100)),
    );
    let more_numbers = worker.new_stream().source(
        "other-iter-source",
        StatelessSource::new(SingleIteratorSource::new(0..=100)),
    );

    numbers
        .union([more_numbers].into_iter())
        .sink("std-out-sink", StatelessSink::new(StdOutSink));
    worker
}
