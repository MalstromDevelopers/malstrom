//! Stream processing can be easy!
use malstrom::operators::*;
use malstrom::runtime::MultiThreadRuntime;
use malstrom::sinks::{StatelessSink, StdOutSink};
use malstrom::snapshot::NoPersistence;
use malstrom::sources::{SingleIteratorSource, StatelessSource};

fn main() {
    MultiThreadRuntime::builder()
        .persistence(NoPersistence)
        .parrallelism(1)
        .build(|runtime| {
            runtime
                .new_stream()
                .source(
                    "iter-source",
                    StatelessSource::new(SingleIteratorSource::new([
                        "Look",
                        "ma'",
                        "I'm",
                        "streaming",
                    ])),
                )
                .map("upper", |x| x.to_uppercase())
                .sink("stdout", StatelessSink::new(StdOutSink));
        })
        .execute()
        .unwrap();
}
