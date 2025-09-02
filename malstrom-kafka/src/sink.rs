use kafka_sink_builder::SetAtLeastOneBroker;
use malstrom::errorhandling::MalstromFatal as _;
use malstrom::sinks::StatelessSinkImpl;
use malstrom::types::DataMessage;
use rdkafka::{producer::BaseProducer, ClientConfig};
use std::{collections::HashMap, time::Duration};
use thiserror::Error;

use bon::bon;
use rdkafka::producer::DefaultProducerContext;

use crate::KafkaRecord;

pub struct KafkaSink {
    producer: BaseProducer<DefaultProducerContext>,
}

#[bon]
impl KafkaSink {
    #[builder]
    #[builder(on(String, into))]
    pub fn new(
        #[builder(field)] kafka_config: HashMap<String, String>,
        #[builder(field)] brokers: Vec<String>,
        /// this is a workaround to check if at least one broker was provided
        #[builder(overwritable, setters(vis = "", name = "at_least_one_broker"))]
        _at_least_one_broker: (),
        group_id: String,
    ) -> Self {
        let mut kafka_conf = ClientConfig::new();
        for (k, v) in kafka_config.iter() {
            kafka_conf.set(k, v);
        }
        let producer = kafka_conf
            .set("group.id", group_id)
            .set("bootstrap.servers", brokers.join(","))
            .create()
            .map_err(KafkaProducerError::CreateProducer)
            .malstrom_fatal();
        Self { producer }
    }
}

impl<S: kafka_sink_builder::State> KafkaSinkBuilder<S> {
    /// Provide an additional config for the Kafka producer.
    /// Note that `bootstrap.servers`, `group.id` configs are
    /// ignored. Use the respective builder methods to supply these.
    pub fn conf(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.kafka_config.insert(key.into(), value.into());
        self
    }
    /// Add a broker URL to produce records to
    pub fn broker(mut self, url: impl Into<String>) -> KafkaSinkBuilder<SetAtLeastOneBroker<S>> {
        self.brokers.push(url.into());
        self.at_least_one_broker(())
    }
}

impl<K, T> StatelessSinkImpl<K, KafkaRecord, T> for KafkaSink {
    fn sink(&mut self, msg: DataMessage<K, KafkaRecord, T>) {
        let record = msg.value;

        let base_record = record.base_record();
        self.producer
            .send(base_record)
            .map_err(|(e, _)| KafkaProducerError::Send(e))
            .malstrom_fatal();
        // TODO: Is it detrimental to poll the producer on every send?
        self.producer.poll(Duration::default());
    }
}

#[derive(Debug, Error)]
pub enum KafkaProducerError {
    #[error("Failed to send message")]
    Send(#[source] rdkafka::error::KafkaError),
    #[error("Failed to create Kafka Producer")]
    CreateProducer(#[source] rdkafka::error::KafkaError),
}

/// Doctests to assert some bad builders do not compile
/// see: https://stackoverflow.com/a/55327334
/// this should not compile because the broker is missing
/// ```compile_fail
/// use malstrom_kafka::KafkaSink;
/// KafkaSink::builder()
/// .group_id("groupid")
/// .build();
/// ```
/// missing group id
/// ```compile_fail
/// use malstrom_kafka::KafkaSink;
/// KafkaSink::builder()
/// .broker("broker.com")
/// .build();
/// ```
struct _CompileTests;

#[cfg(test)]
mod tests {
    use super::KafkaSink;

    #[test]
    fn test_sink_builder() {
        let _sink = KafkaSink::builder()
            .broker("foo.com")
            .group_id("mygroup")
            .conf("log_level", "3")
            .build();
    }
}
