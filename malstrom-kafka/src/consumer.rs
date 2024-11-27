use std::time::Duration;
use jetstream::sinks::StatelessSinkImpl;
use jetstream::types::DataMessage;
use rdkafka::config::ClientConfig;
use rdkafka::producer::BaseProducer;

use bon::bon;
use bon::Builder;
use rdkafka::producer::BaseRecord;
use rdkafka::producer::DefaultProducerContext;

pub struct KafkaProducer {
    producer: BaseProducer<DefaultProducerContext>,
}

#[bon]
impl KafkaProducer {
    #[builder(on(String, into))]
    fn new(brokers: Vec<String>, group_id: String, extra_config: Option<ClientConfig>) -> Self {
        let mut kafka_conf = ClientConfig::new();
        kafka_conf
            .set("group.id", group_id)
            .set("bootstrap.servers", brokers.join(","));
        if let Some(extra) = extra_config.as_ref() {
            for (k, v) in extra.config_map().iter() {
                kafka_conf.set(k, v);
            }
        }
        let producer = kafka_conf.create().unwrap();
        Self { producer }
    }
}

#[derive(Builder)]
pub struct KafkaRecord {
    pub topic: String,
    pub partition: Option<i32>,
    pub payload: Vec<u8>,
    pub key: Vec<u8>,
    pub timestamp: Option<i64>,
}

impl<K, T> StatelessSinkImpl<K, KafkaRecord, T> for KafkaProducer {
    fn sink_message(&mut self, msg: DataMessage<K, KafkaRecord, T>) -> () {
        let record = msg.value;

        let mut base_record = BaseRecord::to(&record.topic)
            .payload(&record.payload)
            .key(&record.key);
        base_record.partition = record.partition;
        base_record.timestamp = record.timestamp;

        self.producer.send(base_record).unwrap();
        self.producer.poll(Duration::default());
    }
}
