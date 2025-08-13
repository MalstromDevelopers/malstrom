use std::collections::HashMap;
use std::time::Duration;

use kafka_source_builder::SetAtLeastOneBroker;
use malstrom::errorhandling::MalstromFatal;
use malstrom::sources::StatefulSourcePartition;

use malstrom::sources::StatefulSourceImpl;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::DefaultConsumerContext;
use rdkafka::consumer::{BaseConsumer, Consumer};

use bon::Builder;
use rdkafka::Message as _;
use rdkafka::TopicPartitionList;
use thiserror::Error;
use tracing::error;

use crate::KafkaRecord;

type OffsetIndex = i64;
type KafkaPartition = i32;

/// Create a new KafkaSource for a given topic.
/// This is a partitioned source with each Kafka partition mapping to one [StatefulSourcePartition].
/// NOTE: Records with an empty payload are not emitted to the stream.
///
/// # Usage
///
/// The source can be instantiated using the builder.
/// Custom [rdkafka configuration](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md)
/// can be provided by calling `.conf(key, value)`.
///
/// ```
/// use malstrom_kafka::KafkaSource;
///
/// let kafka_source = KafkaSource::builder()
///     .broker("mybroker.com") // at least one broker must be provided
///     .broker("myotherbroker.com")
///     .topic("pets")
///     .group_id("my-group-id")
///     .auto_offset_reset("latest")
///     .conf("log_level", "3") // additional custom config
///     .conf("security.protocol", "ssl");
/// ```
#[derive(Builder, Debug)]
#[builder(on(String, into))]
pub struct KafkaSource {
    #[builder(field)]
    kafka_config: HashMap<String, String>,
    #[builder(field)]
    brokers: Vec<String>,
    /// this is a workaround to check if at least one broker was provided
    #[builder(overwritable, setters(vis = "", name = "at_least_one_broker"))]
    _at_least_one_broker: (),
    topic: String,
    group_id: String,
    auto_offset_reset: String,
    /// Timeout for fetching Kafka Broker metadata
    #[builder(default=Duration::from_secs(10))]
    metadata_fetch_timeout: Duration,
}

impl<S: kafka_source_builder::State> KafkaSourceBuilder<S> {
    /// Provide an additional config for the Kafka consumer.
    /// Note that `bootstrap.servers`, `group.id` and `auto.offset.reset` configs are
    /// ignored. Use the respective builder methods to supply these
    pub fn conf(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.kafka_config.insert(key.into(), value.into());
        self
    }
    /// Add a broker URL to consume from
    pub fn broker(mut self, url: impl Into<String>) -> KafkaSourceBuilder<SetAtLeastOneBroker<S>> {
        self.brokers.push(url.into());
        self.at_least_one_broker(())
    }
}

impl StatefulSourceImpl<KafkaRecord, OffsetIndex> for KafkaSource {
    type Part = KafkaPartition;
    type PartitionState = Option<OffsetIndex>;
    type SourcePartition = KafkaSourcePartition;

    fn list_parts(&self) -> Vec<Self::Part> {
        let consumer: BaseConsumer = create_consumer(
            &self.group_id,
            &self.brokers,
            &self.auto_offset_reset,
            &self.kafka_config,
        );
        let metadata = consumer
            .fetch_metadata(Some(&self.topic), self.metadata_fetch_timeout)
            .map_err(KafkaConsumerError::FetchMetadata)
            .malstrom_fatal();
        // idx 0 since we selected a single topic beforehand
        let partitions: Vec<_> = metadata.topics()[0]
            .partitions()
            .iter()
            .map(|x| x.id())
            .collect();
        partitions
    }

    fn build_part(
        &mut self,
        part: &Self::Part,
        part_state: Option<Self::PartitionState>,
    ) -> Self::SourcePartition {
        let consumer: BaseConsumer = create_consumer(
            &self.group_id,
            &self.brokers,
            &self.auto_offset_reset,
            &self.kafka_config,
        );
        let mut topic_partitions = TopicPartitionList::with_capacity(1);
        topic_partitions.add_partition(&self.topic, *part);
        consumer
            .assign(&topic_partitions)
            .map_err(KafkaConsumerError::TopicPartition)
            .malstrom_fatal();
        KafkaSourcePartition::new(consumer, part_state.flatten(), &self.topic, *part)
    }
}

fn create_consumer(
    group_id: &str,
    brokers: &[String],
    auto_offset_reset: &str,
    extra_conf: &HashMap<String, String>,
) -> BaseConsumer {
    let mut kafka_conf = ClientConfig::new();
    for (k, v) in extra_conf.iter() {
        kafka_conf.set(k, v);
    }
    kafka_conf
        .set("group.id", group_id)
        .set("bootstrap.servers", brokers.join(","))
        .set("auto.offset.reset", auto_offset_reset);
    kafka_conf
        .create()
        .map_err(KafkaConsumerError::CreateConsumer)
        .malstrom_fatal()
}

type LastReceivedOffset = Option<i64>;

/// A single partition of [KafkaSource].
/// This type can not be constructed directly, use [KafkaSource] instead.
pub struct KafkaSourcePartition {
    // offset of the last received record
    consumer: BaseConsumer<DefaultConsumerContext>,
    last_recvd_offset: LastReceivedOffset,
    topic: String,
    partition: i32,
}

impl KafkaSourcePartition {
    fn new(
        consumer: BaseConsumer<DefaultConsumerContext>,
        last_recvd_offset: LastReceivedOffset,
        topic: &str,
        partition: i32,
    ) -> Self {
        Self {
            consumer,
            last_recvd_offset,
            topic: topic.to_owned(),
            partition,
        }
    }
}

impl StatefulSourcePartition<KafkaRecord, i64> for KafkaSourcePartition {
    type PartitionState = LastReceivedOffset;

    fn poll(&mut self) -> Option<(KafkaRecord, i64)> {
        let msg = self
            .consumer
            .poll(Duration::default())?
            .map_err(KafkaConsumerError::Poll)
            .malstrom_fatal();
        let offset = msg.offset();
        self.last_recvd_offset = Some(offset);
        KafkaRecord::from_message(&msg).map(|x| (x, offset))
    }

    fn snapshot(&self) -> Self::PartitionState {
        if let Some(offset) = self.last_recvd_offset {
            let mut tpl = TopicPartitionList::new();
            // we let this be a soft error since we still store our offsets in state so it is not
            // lost if there is an error committing it
            let _ = tpl.add_partition_offset(
                &self.topic,
                self.partition,
                rdkafka::Offset::Offset(offset),
            );
            if let Err(e) = self
                .consumer
                .commit(&tpl, rdkafka::consumer::CommitMode::Async)
            {
                error!("Error committing offset to Kafka: {e}");
            }
        }
        self.last_recvd_offset
    }

    fn collect(self) -> Self::PartitionState {
        self.last_recvd_offset
    }

    #[inline(always)]
    fn is_finished(&mut self) -> bool {
        false // kafka is unbounded
    }
}

/// Possible errors which can occur in the KafkaConsumer
#[derive(Debug, Error)]
pub enum KafkaConsumerError {
    #[error("Error polling Kafka consumer")]
    Poll(#[source] rdkafka::error::KafkaError),
    #[error("Failed to create Kafka consumer")]
    CreateConsumer(#[source] rdkafka::error::KafkaError),
    #[error("Could not assign topic-partition to consumer")]
    TopicPartition(#[source] rdkafka::error::KafkaError),
    #[error("Failed to fetch metadata from Kafka broker")]
    FetchMetadata(#[source] rdkafka::error::KafkaError),
}

/// Doctests to assert some bad builders do not compile
/// see: https://stackoverflow.com/a/55327334
/// this should not compile because the broker is missing
/// ```compile_fail
/// use malstrom_kafka::KafkaSource;
/// KafkaSource::builder()
/// .topic("foobar")
/// .group_id("groupid")
/// .auto_offset_reset("earliest")
/// .build();
/// ```
/// missing topic
/// ```compile_fail
/// use malstrom_kafka::KafkaSource;
/// KafkaSource::builder()
/// .group_id("groupid")
/// .auto_offset_reset("earliest")
/// .broker("broker.com")
/// .build();
/// ```
/// missing group id
/// ```compile_fail
/// use malstrom_kafka::KafkaSource;
/// KafkaSource::builder()
/// .topic("foobar")
/// .auto_offset_reset("earliest")
/// .broker("broker.com")
/// .build();
/// ```
/// missing offset reset
/// ```compile_fail
/// use malstrom_kafka::KafkaSource;
/// KafkaSource::builder()
/// .topic("foobar")
/// .group_id("groupid")
/// .broker("broker.com")
/// .build();
/// ```
struct _CompileTests;

#[cfg(test)]
mod tests {
    use super::KafkaSource;

    #[test]
    fn test_builder() {
        let _x = KafkaSource::builder()
            .topic("foobar")
            .group_id("groupid")
            .auto_offset_reset("earliest")
            .broker("foo.com")
            .conf("log_level", "3")
            .build();
    }
}
