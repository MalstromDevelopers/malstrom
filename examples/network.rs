use std::{
    net::{SocketAddr, SocketAddrV4},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use jetstream::{
    inspect::Inspect,
    network_exchange::{NetworkExchange, Remote},
    stream::jetstream::JetStreamEmpty,
    worker::Worker,
};
use tonic::transport::Uri;
use url::Url;

/// This is an example of two Streams (A and B) which exchange
/// data via a network.
///
/// A will produce all numbers from 0..10 and B will produce
/// all numbers from 10..20.
/// They will exchange messages in such a way, that A gets all
/// the even numbers and B gets all the odd numbers.
fn main() {
    let addr_a: SocketAddr = SocketAddr::V4(SocketAddrV4::new("127.0.0.1".parse().unwrap(), 29918));
    let addr_b: SocketAddr = SocketAddr::V4(SocketAddrV4::new("127.0.0.1".parse().unwrap(), 29919));

    let remote_a = Remote::new("A".into(), format!("http://{addr_a}").parse().unwrap());
    let remote_b = Remote::new("B".into(), format!("http://{addr_b}").parse().unwrap());

    let a = std::thread::spawn(move || run_stream_a("A".into(), addr_a, vec![remote_b]));
    let b = std::thread::spawn(move || run_stream_b("B".into(), addr_b, vec![remote_a]));

    a.join().unwrap();
    b.join().unwrap();
}

fn run_stream_a(
    client_id: String,
    this_addr: SocketAddr,
    others: Vec<Remote>,
) {
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

    // Soooo, what is happening here?
    // We are creating an exchange operator, which will
    // listen on `stream_a_addr` and will try to connect
    // to `stream_b_addr` (we could also specify multiple addresses).
    // Arguably the most important part though is the partitioning function:
    // It will send all even values downstream locally, by giving them index 0
    // (local) and all odd values to stream_b
    let stream = stream
        .network_exchange(client_id, this_addr, &others, |data, _count| {
            if (data & 1) == 0 {
                // even
                vec![0]
            } else {
                vec![1]
            }
        })
        .expect("Failed to connect to remote")
        .inspect(|x| println!("Stream A: {x}"))
        .build();

    let mut worker = Worker::new();
    worker.add_stream(stream);

    while worker.get_frontier().unwrap_or(0) < 9 {
        worker.step()
    }
    println!("Stream A done with frontier at {:?}", worker.get_frontier());
    std::thread::sleep(Duration::from_secs(2));
}

fn run_stream_b(
    client_id: String,
    this_addr: SocketAddr,
    others: Vec<Remote>,
) {
    let mut numbers_b = (10..20).into_iter();
    let stream = JetStreamEmpty.source(move |frontier| {
        if let Some(i) = numbers_b.next() {
            frontier.advance_to(i).unwrap();
            Some(i)
        } else {
            None
        }
    });

    let stream = stream
        .network_exchange(client_id, this_addr, &others, |data, _count| {
            if (data & 1) == 0 {
                // even
                vec![1]
            } else {
                vec![0]
            }
        })
        .expect("Failed to connect to remote")
        .inspect(|x| println!("Stream B: {x}"))
        .build();

    let mut worker = Worker::new();
    worker.add_stream(stream);

    while worker.get_frontier().unwrap_or(0) < 9 {
        worker.step()
    }
    println!("Stream B done with frontier at {:?}", worker.get_frontier());
    std::thread::sleep(Duration::from_secs(2));
}
