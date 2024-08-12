use std::iter::once;
use std::thread::JoinHandle;
use std::time::Instant;
use std::u64;

use flume::{Receiver, Sender};
use jetstream::config::Config;
use jetstream::keyed::{rendezvous_select, KeyDistribute};
use jetstream::operators::*;
use jetstream::worker::RuntimeBuilder;
use jetstream::{snapshot::NoPersistence, test::get_test_configs};
use nexmark::config::NexmarkConfig;
use nexmark::event::{Auction, Bid, Event};
use nexmark::EventGenerator;
use serde::{Deserialize, Serialize};
use tracing::{error, info};

fn main() {
    q4::<2>(1_000_000);
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

    // we will send our artificial data into this channel
    let (tx, rx) = flume::bounded::<Event>(8192);

    let threads: Vec<JoinHandle<()>> = configs
        .into_iter()
        .map(|c| (c, rx.clone()))
        .map(|(c, recv)| {
            std::thread::spawn(|| {
                run_stream(c, recv);
            })
        })
        .chain(once(std::thread::spawn(move || {
            generate_events(tx, message_count)
        })))
        .collect();

    let start = Instant::now();
    for t in threads {
        t.join().unwrap();
    }
    let elapsed = Instant::now().duration_since(start);
    info!("Took {elapsed:?} for {message_count} records on {THREAD_CNT} threads");
}

fn generate_events(out: Sender<Event>, message_count: usize) -> () {
    let nex = NexmarkConfig::default();
    let log_interval = message_count / 10;

    for (i, x) in EventGenerator::new(nex).take(message_count).enumerate() {
        let _ = out.send(x);
        if i % log_interval == 0 {
            info!("Remaining {}", message_count - (i + 1));
        }
    }
    drop(out)
}

fn run_stream(config: Config, source: Receiver<Event>) -> () {
    let this = config.worker_id;
    let worker = RuntimeBuilder::new(NoPersistence::default(), || false);

    let (ontime, _) = worker
        .new_stream()
        .source(source.into_iter().enumerate())
        .filter_map(|x| match x.1 {
            Event::Auction(a) => Some((x.0, PartialEvent::Auction(a))),
            Event::Bid(b) => Some((x.0, PartialEvent::Bid(b))),
            Event::Person(_) => None,
        })
        .assign_timestamps_and_convert_epochs(
            move |x| match &x.value.1 {
                PartialEvent::Auction(a) => a.date_time,
                PartialEvent::Bid(b) => b.date_time,
            },
            |t| (t == usize::MAX).then_some(u64::MAX),
        )
        .generate_epochs(move |x, last| {
            // if x.value.0 == msg_count_clnd - 1 {
            //     println!("{} emitting MAX epoch", this);
            //     return Some(u64::MAX);
            // };
            let msg_time = match &x.value.1 {
                PartialEvent::Auction(a) => a.date_time,
                PartialEvent::Bid(b) => b.date_time,
            };
            Some(msg_time)

            // // // emit epoch every second at most
            // if last.map_or(true, |x| (msg_time - x) > 1) {
            //     Some(msg_time)
            // } else {
            //     None
            // }
        });

    let stream = ontime.align_frontiers(2000).map(|x| x.1);
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
        .flexible_window(
            // state: [sum_of_bids, count_of_auctions]
            |msg| Some(([msg.value.max_bid, 1], u64::MAX)),
            |msg, state, _| {
                state[0] += msg.value.max_bid;
                state[1] += 1;
            }
        )
        // .stateful_map(move |_key, auction, mut prices: Vec<usize>| {
        //     prices.push(auction.max_bid);
        //     let avg = prices.iter().sum::<usize>() as f64 / prices.len() as f64;
        //     (avg, Some(prices))
        // })
        // .label(&format!("stateful_map@{}", this))
        .inspect_frontier();

    stream.finish();
    let mut runtime = worker.build(config).unwrap();

    while global_frontier.get_time().map_or(true, |x| x != u64::MAX) {
        runtime.step()
    }
}

#[derive(Clone, Serialize, Deserialize)]
enum PartialEvent {
    Auction(Auction),
    Bid(Bid),
}
