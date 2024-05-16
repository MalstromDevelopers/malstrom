use std::collections::HashMap;
use std::time::Duration;

use indexmap::IndexMap;
use itertools::Itertools;
use jetstream::channels::selective_broadcast::{Receiver, Sender};
use jetstream::keyed::{rendezvous_select, KeyDistribute};
use jetstream::operators::map::Map;
use jetstream::operators::source::{self, Source};
use jetstream::operators::stateful_map::StatefulMap;
use jetstream::stream::jetstream::JetStreamBuilder;
use jetstream::stream::operator::{BuildContext, OperatorBuilder, OperatorContext};
use jetstream::time::NoTime;
use jetstream::{DataMessage, Message, NoData, NoKey};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::ConsumerContext;
use rdkafka::consumer::{BaseConsumer, Consumer, StreamConsumer};
use rdkafka::error::KafkaError;
use rdkafka::message::{BorrowedMessage, OwnedMessage};
use rdkafka::metadata::MetadataPartition;
use rdkafka::{ClientContext, Offset};
use serde::{Deserialize, Serialize};
use rdkafka::consumer::DefaultConsumerContext;
use rdkafka::{TopicPartitionList};

struct DefaultKafkaContext;
impl ClientContext for DefaultKafkaContext {}
impl ConsumerContext for DefaultKafkaContext {}

type PartitionIndex = i32;

pub trait KafkaInput {
    fn kafka_input(
        self,
        brokers: Vec<String>,
        group_id: String,
        topic: String,
        auto_offset_reset: String,
        metadata_timeout: Duration,
    ) -> JetStreamBuilder<PartitionIndex, Result<OwnedMessage, KafkaError>, NoTime>;
}

impl KafkaInput for JetStreamBuilder<NoKey, NoData, NoTime> {
    fn kafka_input(
        self,
        brokers: Vec<String>,
        group_id: String,
        topic: String,
        auto_offset_reset: String,
        metadata_timeout: Duration,
    ) -> JetStreamBuilder<PartitionIndex, Result<OwnedMessage, KafkaError>, NoTime> {
        let consumer: BaseConsumer = ClientConfig::new()
            .set("group.id", group_id.clone())
            .set("bootstrap.servers", brokers.join(","))
            .set("enable.partition.eof", "false")
            .create()
            .expect("Failed to create Kafka consumer");

        let metadata = consumer
            .fetch_metadata(Some(&topic), metadata_timeout)
            .expect("Failed to fetch metadata");
        // idx 0 since we selected a single topic beforehand
        let partitions = metadata.topics()[0].partitions().iter().map(|x| x.id()).collect_vec();

        self.source(partitions)
            .key_distribute(
                |x| x.value.clone(),
                |k, workers| rendezvous_select(k, workers.iter()).unwrap(),
            )
            .then(OperatorBuilder::built_by(|ctx| build_consumer(brokers, topic, group_id, auto_offset_reset, ctx)))
    }
}

#[derive(Default, Serialize, Deserialize)]
struct ConsumerState {
    // offset of the last received record
    last_recvd_offset: Option<i64>,
}


fn build_consumer(
    brokers: Vec<String>,
    topic: String,
    group_id: String,
    auto_offset_reset: String,
    context: &mut BuildContext,
) -> impl FnMut(
    &mut Receiver<PartitionIndex, PartitionIndex, NoTime>,
    &mut Sender<PartitionIndex, Result<OwnedMessage, KafkaError>, NoTime>,
    &mut OperatorContext,
) {
    let mut state: IndexMap<PartitionIndex, ConsumerState> =
        context.load_state().unwrap_or_default();
    let mut consumers: HashMap<PartitionIndex, BaseConsumer> = HashMap::new();

    move |input, output, ctx| {
        let msg = match input.recv() {
            Some(x) => x,
            None => return,
        };
        match msg {
            Message::Data(d) => {
                state.insert(d.value, ConsumerState::default());
            }
            // mostly copied from stateful_map
            Message::Interrogate(mut x) => {
                x.add_keys(&(state.keys().map(|k| k.to_owned()).collect_vec()));
                output.send(Message::Interrogate(x))
            }
            Message::Collect(mut c) => {
                if let Some(x) = state.get(&c.key) {
                    c.add_state(ctx.operator_id, x);
                }
                // we need to remove the consumer eagerly to avoid
                // reading duplicates
                state.shift_remove(&c.key);
                consumers.remove(&c.key);
                output.send(Message::Collect(c))
            }
            Message::Acquire(a) => {
                if let Some(st) = a.take_state(&ctx.operator_id) {
                    state.insert(st.0, st.1);
                }
                output.send(Message::Acquire(a))
            }
            Message::DropKey(k) => {
                // already removed in collect
                output.send(Message::DropKey(k))
            }
            // necessary to convince Rust it is a different generic type now
            Message::AbsBarrier(mut b) => {
                b.persist(&state, &ctx.operator_id);
                output.send(Message::AbsBarrier(b))
            }
            Message::Rescale(x) => output.send(Message::Rescale(x)),
            Message::ShutdownMarker(x) => output.send(Message::ShutdownMarker(x)),
            Message::Epoch(x) => output.send(Message::Epoch(x)),
        };

        for (partition_idx, offset) in state.iter() {
            let consumer = consumers.entry(*partition_idx).or_insert_with(|| create_and_subscribe_consumer(
                brokers.clone(), 
                topic.clone(),
                group_id.clone(),
                auto_offset_reset.clone(),
                offset.last_recvd_offset.map(|x| x + 1),
                *partition_idx
            ));
            if let Some(record) = consumer.poll(Duration::default()).map(|res| res.map(|msg| msg.detach())) {
                output.send(Message::Data(DataMessage::new(*partition_idx, record, NoTime)))
            }
        }
    }
}


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
    brokers: Vec<String>,
    topic: String,
    group_id: String,
    auto_offset_reset: String,
    starting_offset: Option<i64>,
    partition: i32,
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
    topic_parititon.add_partition(&topic, partition);
    consumer.assign(&topic_parititon).expect("Failed to subscribe to topic");
    if let Some(offset) = starting_offset {
        consumer.seek(&topic, partition, Offset::Offset(offset), Duration::from_secs(16)).expect("Error setting partition offset");
    }
    consumer
}