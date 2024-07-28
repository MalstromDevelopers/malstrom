use jetstream::config::Config;
use jetstream::operators::filter::Filter;
use jetstream::operators::map::Map;
use jetstream::{snapshot::NoPersistence, test::get_test_configs, InnerRuntimeBuilder};
use jetstream_kafka::consumer::KafkaInput;
use rdkafka::Message as _;
use serde::{Deserialize, Serialize};
use serde_json::de::from_slice;
use std::process::exit;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use std::time::{Duration, Instant};

fn main() {
    let [config0, config1] = get_test_configs::<2>();

    let counter = Arc::new(AtomicU32::default());
    let counter1 = counter.clone();
    let threads = [
        std::thread::spawn(move || run_stream(config0, counter)),
        std::thread::spawn(move || run_stream(config1, counter1)),
    ];
    let start_ts = Instant::now();
    for x in threads {
        let _ = x.join();
    }
    let end_ts = Instant::now();
    let diff = end_ts.duration_since(start_ts);
    println!("Took {diff:?}");
}

fn run_stream(config: Config, counter: Arc<AtomicU32>) -> () {
    let mut worker = InnerRuntimeBuilder::new(NoPersistence::default(), || false);

    let total = 78_200_000;
    let counter_moved = counter.clone();
    let stream = worker
        .new_stream()
        .kafka_source(
            vec!["localhost:9092".into()],
            "jetstream.example".into(),
            "nexmark-bid".into(),
            "earliest".into(),
            Duration::from_secs(3),
        )
        .filter(|x| x.is_ok())
        // decode messages
        .map(|x| {
            let record = x.unwrap();
            let payload = record.payload().unwrap();
            let decoded: Bid = from_slice(payload).unwrap();
            decoded
        })
        .map(|mut x| {
            x.price = (x.price as f64 * 0.908) as usize;
            x
        })
        .map(move |_| counter_moved.fetch_add(1, std::sync::atomic::Ordering::Relaxed));

    worker.add_stream(stream);
    let mut runtime = worker.build(config).unwrap();
    loop {
        let cnt = counter.load(std::sync::atomic::Ordering::Relaxed);
        if cnt >= total {
            return;
        }
        // if (cnt % 100_000) == 0 {
        //     println!("{cnt}");
        // }
        runtime.step()
    }
}

type Id = usize;

#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
pub struct Bid {
    /// The ID of the auction this bid is for.
    pub auction: Id,
    /// The ID of the person that placed this bid.
    pub bidder: Id,
    /// The price in cents that the person bid for.
    pub price: usize,
    /// The channel of this bid
    pub channel: String,
    /// The url of this bid
    pub url: String,
    /// A millisecond timestamp for the event origin.
    pub date_time: String,
    /// Extra information
    pub extra: String,
}
