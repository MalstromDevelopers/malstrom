use jetstream::{
    inspect::Inspect, network_exchange::NetworkExchange, stream::jetstream::JetStreamEmpty,
    worker::Worker,
};
use url::Url;

const ADDRESS_A: &'static str = "inproc://nng/stream_a";
const ADDRESS_B: &'static str = "inproc://nng/stream_a";

/// This is an example of two Streams (A and B) which exchange
/// data via a network.
///
/// A will produce all numbers from 0..10 and B will produce
/// all numbers from 10..20.
/// They will exchange messages in such a way, that A gets all
/// the even numbers and B gets all the odd numbers.
fn main() {
    let a = std::thread::spawn(run_stream_a);
    let b = std::thread::spawn(run_stream_b);

    a.join().unwrap();
    b.join().unwrap();
}

fn run_stream_a() {
    // We will set up two streams, one producing the numbers
    // from 0..10 and another producing the numbers 10..20
    let mut numbers_a = (0..10).into_iter();
    let stream = JetStreamEmpty.source(move |frontier| {
        if let Some(i) = numbers_a.next() {
            frontier.advance_to(i).unwrap();
            Some(i)
        } else {
            None
        }
    });

    // next we will set up communication for both streams
    let stream_a_addr = Url::parse(ADDRESS_A).unwrap();
    let stream_b_addr = Url::parse(ADDRESS_B).unwrap();

    // Soooo, what is happening here?
    // We are creating an exchange operator, which will
    // listen on `stream_a_addr` and will try to connect
    // to `stream_b_addr` (we could also specify multiple addresses).
    // Arguably the most important part though is the partitioning function:
    // It will send all even values downstream locally, by giving them index 0
    // (local) and all odd values to stream_b
    let stream = stream
        .network_exchange(stream_a_addr, &[stream_b_addr], |data, _count| {
            if (data & 1) == 0 {
                // even
                vec![0]
            } else {
                vec![1]
            }
        })
        .expect("Failed to connect to remote");

    // we will attach an inspect to see our data
    let stream = stream.inspect(|x| println!("Stream A: {x}")).build();

    let mut worker = Worker::new();
    worker.add_stream(stream);

    for _ in 0..20 {
        worker.step()
    }
}

fn run_stream_b() {
    let mut numbers_b = (10..20).into_iter();
    let stream = JetStreamEmpty.source(move |frontier| {
        if let Some(i) = numbers_b.next() {
            frontier.advance_to(i).unwrap();
            Some(i)
        } else {
            None
        }
    });

    // next we will set up communication for both streams
    let stream_a_addr = Url::parse(ADDRESS_A).unwrap();
    let stream_b_addr = Url::parse(ADDRESS_B).unwrap();

    let stream = stream
        .network_exchange(stream_b_addr, &[stream_a_addr], |data, _count| {
            if (data & 1) == 0 {
                // even
                vec![1]
            } else {
                vec![0]
            }
        })
        .expect("Failed to connect to remote");

    // we will attach an inspect to see our data
    let stream = stream.inspect(|x| println!("Stream B: {x}")).build();

    let mut worker = Worker::new();
    worker.add_stream(stream);

    for _ in 0..20 {
        worker.step()
    }
}
