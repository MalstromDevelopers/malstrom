//! Stream processing can be easy!
use malstrom::operators::*;
use malstrom::runtime::MultiThreadRuntime;
use malstrom::sinks::{StatelessSink, StdOutSink};
use malstrom::snapshot::NoPersistence;
use malstrom::sources::{SingleIteratorSource, StatelessSource};
use malstrom::worker::StreamProvider;

fn main() {
    MultiThreadRuntime::builder()
        .persistence(NoPersistence)
        .parrallelism(1)
        .build(build_dataflow)
        .execute()
        .unwrap();
}

fn build_dataflow(provider: &mut dyn StreamProvider) {
    provider
        .new_stream()
        .source(
            "words",
            StatelessSource::new(SingleIteratorSource::new([
                "Look",
                "ma'",
                "I'm",
                "streaming",
            ])),
        )
        .map("upper", |x| x.to_uppercase())
        .sink("stdout", StatelessSink::new(StdOutSink));
}
