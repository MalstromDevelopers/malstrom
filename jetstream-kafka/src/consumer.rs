use std::time::Duration;

use rdkafka::consumer::{BaseConsumer, Consumer, StreamConsumer};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::ConsumerContext;
use rdkafka::ClientContext;

struct DefaultKafkaContext;
impl ClientContext for DefaultKafkaContext{}
impl ConsumerContext for DefaultKafkaContext{}

fn kafka_input(brokers: &str, group_id: &str, topic: &str, metadata_timeout: Duration) -> () {
    let consumer: BaseConsumer = ClientConfig::new()
    .set("group.id", group_id)
    .set("bootstrap.servers", brokers)
    .set("enable.partition.eof", "false")
    .create()
    .expect("Failed to create Kafka consumer");

    let metadata = consumer.fetch_metadata(Some(topic), metadata_timeout).expect("Failed to fetch metadata");
    let partitions = metadata.topics()[0].name()
}