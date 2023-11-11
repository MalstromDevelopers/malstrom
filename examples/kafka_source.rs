use jetstream::{worker::Worker, stream::jetstream::JetStreamEmpty, kafka::KafkaSource, map::Map, filter::Filter, inspect::Inspect};
use rdkafka::Message;

fn decode(bytes: &[u8]) -> &str {
    std::str::from_utf8(bytes).unwrap()
}

fn main() {
    let mut worker = Worker::new();

    let brokers = vec!("localhost:8083");
    let topic = "owlshop-customers";
    let group_id = "jetstream";
    let auto_offset_reset = "earliest";
    let partitions = None;

    let stream = JetStreamEmpty
    .kafka_source(brokers, topic, group_id, auto_offset_reset, partitions)
    .map(|msg|  decode(msg.payload().unwrap()).to_owned())
    .inspect(|msg| println!("{msg}"))
    .build()
    ;

    worker.add_stream(stream);

    loop {
        worker.step()
    }
}