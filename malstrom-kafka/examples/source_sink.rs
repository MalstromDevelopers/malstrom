//! A basic example of reading from and writing to Kafka
use malstrom::operators::{Inspect, Sink};
use malstrom::sinks::StatelessSink;
use malstrom::snapshot::NoPersistence;
use malstrom::sources::StatefulSource;
use malstrom::worker::StreamProvider;
use malstrom::{operators::Source, runtime::SingleThreadRuntime};
use malstrom_kafka::{KafkaSink, KafkaSource};

fn main() {
    let _rt = SingleThreadRuntime::builder()
        .persistence(NoPersistence)
        .build(build_dataflow);
}

fn build_dataflow(provider: &mut dyn StreamProvider) -> () {
    let source = KafkaSource::builder()
        .broker("some-broker-url.com")
        .broker("some-other-broker-url.com")
        .topic("foobar")
        .group_id("my-group")
        .auto_offset_reset("latest")
        // https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md
        // for all supported config values
        .conf("max.in.flight", "10000")
        .build();
    let sink = KafkaSink::builder()
        .broker("sink-broker.com")
        .group_id("my-group")
        .build();

    provider
        .new_stream()
        .source("kafka-source", StatefulSource::new(source))
        .inspect("print", |msg, _| println!("{msg:?}"))
        .sink("kafka-sink", StatelessSink::new(sink));
}
