use std::{
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    thread::sleep,
    time::Duration,
};

use postbox::BackendBuilder;
use tonic::transport::Uri;

fn main() {
    let listen_addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), 29091));
    let peer: Uri = "http://localhost:29091".parse().unwrap();
    let worker_id = String::from("Arthur Dent");

    let backend = BackendBuilder::new(
        worker_id.clone(),
        listen_addr,
        vec![(worker_id.clone(), peer)],
        vec![0],
        128,
    );
    let op_postbox = backend.for_operator(&0).unwrap();

    let _backend = backend.connect().unwrap();

    op_postbox.send(&worker_id, vec![1u8, 2, 3, 4]).unwrap();

    loop {
        match op_postbox.recv_all::<Vec<u8>>().next() {
            Some(_x) => {
                break;
            }
            None => {
                sleep(Duration::from_millis(100));
                continue;
            }
        }
    }
}
