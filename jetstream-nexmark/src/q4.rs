use std::sync::atomic::{AtomicI16, AtomicUsize};
use std::sync::Arc;
use std::time::Instant;

use jetstream::config::Config;
use jetstream::keyed::{rendezvous_select, KeyDistribute};
use jetstream::operators::*;
use jetstream::worker::RuntimeBuilder;
use jetstream::{snapshot::NoPersistence, test::get_test_configs};
use nexmark::config::NexmarkConfig;
use nexmark::event::{Auction, Bid, Event};
use nexmark::EventGenerator;
use serde::{Deserialize, Serialize};
use std::sync::atomic::Ordering::Relaxed;
use tracing::{error, info};

fn main() {
    q4::<3>(10_000_000);
}

#[derive(Default, Clone, Serialize, Deserialize)]
struct AuctionState {
    id: usize,
    max_bid: usize,
    expires: u64,
    category: usize,
}

impl AuctionState {
    pub fn new(id: usize, max_bid: usize, expires: u64, category: usize) -> Self {
        Self {
            id,
            max_bid,
            expires,
            category,
        }
    }
}

/// Runs the q4 nexmark benchmark:
/// Select the average of the wining bid prices for all auctions in each category.
// Illustrates complex join and aggregation.
fn q4<const THREAD_CNT: usize>(message_count: usize) -> () {
    env_logger::init();
    error!("Starting log");
    let configs = get_test_configs::<THREAD_CNT>();
    let cnt_per_thread = message_count / THREAD_CNT;

    let coordination = Arc::new(AtomicI16::new(8));

    let threads = configs
        .map(|c| (c, cnt_per_thread.clone(), coordination.clone()))
        .map(|(c, x, coord)| {
            std::thread::spawn(move || {
                let wid = c.worker_id;
                run_stream(c, x, coord);
                println!("{wid} finished");
            })
        });

    let start = Instant::now();
    for t in threads {
        t.join();
    }
    let elapsed = Instant::now().duration_since(start);
    info!("Took {elapsed:?} for {message_count} records on {THREAD_CNT} threads");
}

fn run_stream(config: Config, msg_count: usize, _coordination: Arc<AtomicI16>) -> () {
    let worker = RuntimeBuilder::new(NoPersistence::default(), || false);

    let nex = NexmarkConfig::default();
    let remaining = Arc::new(AtomicUsize::new(msg_count));
    let remaining_cloned = remaining.clone();

    let msg_count_clnd = msg_count.clone();

    let gen = EventGenerator::new(nex)
        .inspect(move |_| {
            remaining_cloned.fetch_sub(1, Relaxed);
        })
        .take(msg_count);
    let this = config.worker_id;

    let (ontime, _) = worker
        .new_stream()
        .source(gen.enumerate())
        .filter_map(|x| match x.1 {
            Event::Auction(a) => Some((x.0, PartialEvent::Auction(a))),
            Event::Bid(b) => Some((x.0, PartialEvent::Bid(b))),
            Event::Person(_) => None,
        })
        .assign_timestamps(move |x| {
            match &x.value.1 {
                PartialEvent::Auction(a) => a.date_time,
                PartialEvent::Bid(b) => b.date_time,
            }
        })
        .generate_epochs(move |x, last| {
            if x.value.0 == msg_count_clnd - 1 {
                println!("{} emitting MAX epoch", this);
                return Some(u64::MAX);
            };
            let msg_time = match &x.value.1 {
                PartialEvent::Auction(a) => a.date_time,
                PartialEvent::Bid(b) => b.date_time,
            };
            // // emit epoch every second at most
            if last.map_or(true, |x| (msg_time - x) > 1) {
                Some(msg_time)
            } else {
                None
            }
        });

    let stream = ontime.map(|x| x.1);
    let (stream, local_frontier) = stream.inspect_frontier();

    let (stream, global_frontier) = stream
        .key_distribute(
            |x| match &x.value {
                PartialEvent::Auction(a) => a.id,
                PartialEvent::Bid(b) => b.auction,
            },
            |key, workers| rendezvous_select(key, workers.iter()).unwrap(),
        )
        .flexible_window(
            |msg| match &msg.value {
                PartialEvent::Auction(a) => {
                    Some((AuctionState::new(a.id, 0, a.expires, a.category), a.expires))
                }
                _ => None,
            },
            |msg, state, end| {
                // don't allow late bids
                if msg.timestamp > *end {
                    return;
                }
                if let PartialEvent::Bid(x) = msg.value {
                    state.max_bid = state.max_bid.max(x.price);
                }
            },
        )
        .key_distribute(
            |x| x.value.category,
            |key, targets| rendezvous_select(key, targets.iter()).unwrap(),
        )
        .stateful_map(move |_key, auction, mut prices: Vec<usize>| {
            prices.push(auction.max_bid);
            let avg = prices.iter().sum::<usize>() as f64 / prices.len() as f64;
            (avg, Some(prices))
        })
        .label(&format!("stateful_map@{}", this))
        .inspect_frontier();

    stream.finish();
    let this = config.worker_id;
    let mut runtime = worker.build(config).unwrap();

    let mut last_print = msg_count;
    while global_frontier.get_time().map_or(true, |x| x != u64::MAX) {
        // loop {
        let r = remaining.load(Relaxed);

        if (last_print - r) > 100_000 {
            let gf = global_frontier.get_time();
            info!("{this}: Remaining {r:?} GF {gf:?}");
            last_print = r;
        }

        runtime.step()
    }

    println!(
        "{:?} Local Frontier: {:?} / {} Global: {:?}",
        this,
        local_frontier.get_time(),
        usize::MAX,
        global_frontier.get_time()
    );
}

#[derive(Clone, Serialize, Deserialize)]
enum PartialEvent {
    Auction(Auction),
    Bid(Bid)
}