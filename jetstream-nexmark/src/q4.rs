use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicI16, AtomicUsize};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use jetstream::channels::selective_broadcast::{Receiver, Sender};
use jetstream::config::Config;
use jetstream::keyed::{rendezvous_select, KeyDistribute};
use jetstream::operators::filter::Filter;
use jetstream::operators::map::Map;
use jetstream::operators::source::Source;
use jetstream::operators::stateful_map::StatefulMap;
use jetstream::operators::timely::{GenerateEpochs, InspectFrontier, TimelyStream};
use jetstream::operators::window::flexible::FlexibleWindow;
use jetstream::stream::operator::OperatorBuilder;
use jetstream::time::NoTime;
use jetstream::{snapshot::NoPersistence, test::get_test_configs, Worker};
use jetstream::{DataMessage, Message, NoKey, ShutdownMarker};
use nexmark::config::NexmarkConfig;
use nexmark::event::Event;
use nexmark::EventGenerator;
use serde::{Deserialize, Serialize};
use std::sync::atomic::Ordering::Relaxed;
use tracing::{debug, error, info};

fn main() {
    q4::<2>(10_000_000);
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

fn run_stream(config: Config, msg_count: usize, coordination: Arc<AtomicI16>) -> () {
    let mut worker = Worker::new(NoPersistence::default(), || false);

    let nex = NexmarkConfig::default();
    let remaining = Arc::new(AtomicUsize::new(msg_count));
    let remaining_cloned = remaining.clone();
    let gen = EventGenerator::new(nex)
        .inspect(move |_| {
            remaining_cloned.fetch_sub(1, Relaxed);
        })
        // .inspect(|_| println!("TAKE"))
        .take(msg_count);
    let this = config.worker_id;

    let (stream, _) = worker
        .new_stream()
        .source(gen)
        // .then(OperatorBuilder::direct(|i, o, _| {
        //     println!("Shed");
        //     if let Some(x) = i.recv() {
        //         o.send(x)
        //     }
        // }))
        .filter(|x| match x {
            Event::Auction(_) => true,
            Event::Bid(_) => true,
            Event::Person(_) => false,
        })
        .assign_and_convert(
            |x| match &x.value {
                Event::Auction(a) => a.date_time,
                Event::Bid(b) => b.date_time,
                Event::Person(_) => unreachable!(),
            },
            move |x| {
                if x == usize::MAX {
                    println!("{} issuing max", this);
                    Some(u64::MAX)
                } else {
                    None
                }
            },
        )
        .generate_epochs(&mut worker, |x, last| {
            let msg_time = match &x.value {
                Event::Auction(a) => a.date_time,
                Event::Bid(b) => b.date_time,
                Event::Person(_) => unreachable!(),
            };

            // // emit epoch every second at most
            if last.map_or(true, |x| (msg_time - x) > 1) {
                Some(msg_time)
            } else {
                None
            }
        });

    let (stream, local_frontier) = stream.inspect_frontier();
    // the event time monotonically advances the epoch, with out of order
    // messages getting dropped
    // let (stream, _late) = stream.generate_epochs(&mut worker, move |x, _| match &x.value {
    //     Event::Auction(a) => Some(a.date_time),
    //     Event::Bid(b) => Some(b.date_time),
    //     Event::Person(_) => unreachable!(),
    // });

    let (stream, global_frontier) = stream
        .key_distribute(
            |x| match &x.value {
                Event::Auction(a) => a.id,
                Event::Bid(b) => b.auction,
                Event::Person(_) => unreachable!(),
            },
            |key, workers| rendezvous_select(key, workers.iter()).unwrap(),
        )
        .flexible_window(
            |msg| match &msg.value {
                Event::Auction(a) => {
                    Some((AuctionState::new(a.id, 0, a.expires, a.category), a.expires))
                }
                _ => None,
            },
            |msg, state, end| {
                // don't allow late bids
                if msg.timestamp > *end {
                    return;
                }
                if let Event::Bid(x) = msg.value {
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
        .inspect_frontier();

    worker.add_stream(stream);
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
