from kafka import KafkaConsumer, KafkaProducer

brokers = [
    "localhost:9092"
]

consumer = KafkaConsumer(
    "nexmark-auction",
    bootstrap_servers=brokers,
    auto_offset_reset="earliest"
)
print(consumer.partitions_for_topic("nexmark-auction"))
for x in consumer:
    print(x)