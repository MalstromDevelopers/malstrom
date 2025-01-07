use std::time::Duration;

use malstrom::sources::StatefulSourcePartition;

use malstrom::sources::StatefulSourceImpl;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::DefaultConsumerContext;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::OwnedMessage;

use bon::Builder;
use rdkafka::Message as _;
use rdkafka::TopicPartitionList;

type OffsetIndex = i64;
type KafkaPartition = i32;

#[derive(Builder)]
#[builder(on(String, into))]
pub struct KafkaSource {
    brokers: Vec<String>,
    topic: String,
    group_id: String,
    auto_offset_reset: String,
    /// Timeout for fetching Kafka Broker metadata
    #[builder(default=Duration::from_secs(30))]
    fetch_timeout: Duration,
    extra_config: Option<ClientConfig>,
}

impl StatefulSourceImpl<OwnedMessage, OffsetIndex> for KafkaSource {
    type Part = KafkaPartition;
    type PartitionState = Option<OffsetIndex>;
    type SourcePartition = KafkaConsumer;

    fn list_parts(&self) -> impl IntoIterator<Item = Self::Part> + 'static {
        let consumer: BaseConsumer = create_consumer(
            &self.group_id,
            &self.brokers,
            &self.auto_offset_reset,
            &self.extra_config,
        );
        let metadata = consumer
            .fetch_metadata(Some(&self.topic), self.fetch_timeout)
            .expect("Can fetch metadata");
        // idx 0 since we selected a single topic beforehand
        let partitions: Vec<_> = metadata.topics()[0]
            .partitions()
            .iter()
            .map(|x| x.id())
            .collect();
        partitions
    }

    fn build_part(
        &self,
        part: &Self::Part,
        part_state: Option<Self::PartitionState>,
    ) -> Self::SourcePartition {
        let consumer: BaseConsumer = create_consumer(
            &self.group_id,
            &self.brokers,
            &self.auto_offset_reset,
            &self.extra_config,
        );
        let mut topic_partitions = TopicPartitionList::with_capacity(1);
        topic_partitions.add_partition(&self.topic, *part);
        consumer
            .assign(&topic_partitions)
            .expect("Can assign topic-partition");
        KafkaConsumer::new(consumer, part_state.flatten())
    }
}

fn create_consumer(
    group_id: &str,
    brokers: &[String],
    auto_offset_reset: &str,
    extra_config: &Option<ClientConfig>,
) -> BaseConsumer {
    let mut kafka_conf = ClientConfig::new();
    kafka_conf
        .set("group.id", group_id)
        .set("bootstrap.servers", brokers.join(","))
        .set("auto.offset.reset", auto_offset_reset)
        .set("enable.partition.eof", "false");

    if let Some(extra) = extra_config.as_ref() {
        for (k, v) in extra.config_map().iter() {
            kafka_conf.set(k, v);
        }
    }
    kafka_conf
        .create()
        .expect("Failed to create Kafka consumer")
}

type LastReceivedOffset = Option<i64>;
pub struct KafkaConsumer {
    // offset of the last received record
    consumer: BaseConsumer<DefaultConsumerContext>,
    last_recvd_offset: LastReceivedOffset,
}

impl KafkaConsumer {
    fn new(
        consumer: BaseConsumer<DefaultConsumerContext>,
        last_recvd_offset: LastReceivedOffset,
    ) -> Self {
        Self {
            consumer,
            last_recvd_offset,
        }
    }
}

impl StatefulSourcePartition<OwnedMessage, i64> for KafkaConsumer {
    type PartitionState = LastReceivedOffset;

    fn poll(&mut self) -> Option<(OwnedMessage, i64)> {
        let msg = self
            .consumer
            .poll(Duration::default())?
            .expect("Can poll Kafka");
        self.last_recvd_offset = Some(msg.offset());
        Some((msg.detach(), msg.offset()))
    }

    fn snapshot(&self) -> Self::PartitionState {
        self.last_recvd_offset
    }

    fn collect(self) -> Self::PartitionState {
        self.last_recvd_offset
    }
}
