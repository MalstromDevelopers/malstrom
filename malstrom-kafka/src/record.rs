use bon::Builder;
use rdkafka::{
    message::BorrowedMessage, producer::BaseRecord, Message
};

/// A single record as received by or sent to Kafka
#[derive(Builder, Debug, Clone)]
pub struct KafkaRecord {
    pub topic: String,
    pub partition: Option<i32>,
    pub payload: Vec<u8>,
    pub key: Option<Vec<u8>>,
    pub timestamp: Option<i64>,
}

impl KafkaRecord {
    
    pub(crate) fn from_message(msg: &BorrowedMessage<'_>) -> Option<Self> {
        let payload = msg.payload().map(|x| x.to_vec())?;
        Some(Self {
            topic: msg.topic().to_owned(),
            partition: Some(msg.partition()),
            payload,
            key: msg.key().map(|x| x.to_vec()),
            timestamp: msg.timestamp().to_millis(),
        })
    }

    pub(crate) fn base_record<'a>(&'a self) -> BaseRecord<'a, Vec<u8>, Vec<u8>> {
        let mut base_record = BaseRecord::<Vec<u8>, Vec<u8>, ()>::to(&self.topic);
        base_record.partition = self.partition;
        base_record.payload = Some(&self.payload);
        base_record.key = self.key.as_ref();
        base_record
    }
}
