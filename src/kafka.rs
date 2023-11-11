use std::time::Duration;

use crate::channels::selective_broadcast::{Receiver, Sender};
use crate::frontier::FrontierHandle;
use crate::stream::jetstream::{Data, JetStreamBuilder, JetStreamEmpty, Nothing};
use crate::stream::operator::StandardOperator;
use rdkafka::message::OwnedMessage;
use rdkafka::{
    consumer::{BaseConsumer, Consumer, DefaultConsumerContext},
    ClientConfig,
};
use rdkafka::{Message, TopicPartitionList};
use tracing::{event, instrument, Level};

/// Create a consumer from a given Kafka topic and broker.
/// The function returns a consumer which is already subscribed
/// to the given topic.
///
/// # Example
/// ```rust
/// let consumer: BaseConsumer<DefaultConsumerContext> =
///     create_and_subscribe_consumer(brokers, topic);
/// ```
pub fn create_and_subscribe_consumer(
    brokers: Vec<&str>,
    topic: &str,
    group_id: &str,
    auto_offset_reset: &str,
    partitions: Option<&[i32]>,
) -> BaseConsumer<DefaultConsumerContext> {
    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", brokers.join(","))
        .set("group.id", group_id)
        .set("auto.offset.reset", auto_offset_reset)
        .set("enable.auto.commit", "false")
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000");

    let consumer: BaseConsumer<DefaultConsumerContext> =
        config.create().expect("Could not create consumer");

    // subscribe to topic
    let mut topic_parititon = TopicPartitionList::new();
    if let Some(parts) = partitions {
        for p in parts.iter() {
            topic_parititon.add_partition(topic, p.to_owned());
        }
        consumer
            .assign(&topic_parititon)
            .expect("Failed to subscribe to topic");
    } else {
        consumer
            .subscribe(&[topic])
            .expect("Failed to subscribe to topic");
    }
    consumer
}

pub trait KafkaSource {
    fn kafka_source(
        self,
        brokers: Vec<&str>,
        topic: &str,
        group_id: &str,
        auto_offset_reset: &str,
        partitions: Option<&[i32]>,
    ) -> JetStreamBuilder<OwnedMessage>;
}

#[instrument(skip_all)]
fn kafka_poll(consumer: &BaseConsumer) -> Option<OwnedMessage> {
    match consumer.poll(Duration::default()) {
        Some(Ok(borrowed_msg)) => Some(borrowed_msg.detach()),
        Some(Err(e)) => {
            println!("Kafka Error: {}", e);
            event!(Level::ERROR, "Kafka Error: {}", e);
            None
        }
        None => None,
    }
}

impl KafkaSource for JetStreamEmpty {
    fn kafka_source(
        self,
        brokers: Vec<&str>,
        topic: &str,
        group_id: &str,
        auto_offset_reset: &str,
        partitions: Option<&[i32]>,
    ) -> JetStreamBuilder<OwnedMessage> {
        let consumer =
            create_and_subscribe_consumer(brokers, topic, group_id, auto_offset_reset, partitions);

        let operator = StandardOperator::new(
            move |_input: &mut Receiver<Nothing>,
                  output: &mut Sender<OwnedMessage>,
                  frontier: &mut FrontierHandle| {
                if let Some(msg) = kafka_poll(&consumer) {
                    // should always succeed
                    let _ = frontier.advance_to(
                        msg.offset()
                            .try_into()
                            .expect("Received negative message offset from Kafka"),
                    );
                    output.send(msg);
                }
            },
        );

        JetStreamBuilder::from_operator(operator)
    }
}
