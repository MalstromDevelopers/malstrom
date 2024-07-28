export KAFKA_HOST="localhost:9092"
export BASE_TOPIC="nexmark-events"
export AUCTION_TOPIC="nexmark-auction"
export BID_TOPIC="nexmark-bid"
export PERSON_TOPIC="nexmark-person"
export NUM_PARTITIONS=3
export SEPARATE_TOPICS=true
export RUST_LOG="nexmark_server=info"

nexmark-server -c
nexmark-server --event-rate 400000 --max-events 400000
