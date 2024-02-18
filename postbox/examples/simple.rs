use std::{
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    thread::sleep,
    time::Duration,
};

use postbox::{BackendBuilder, Message};
use tonic::transport::Uri;

fn main() {
    let listen_addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), 29091));
    let peer: Uri = "http://localhost:29091".parse().unwrap();
    let backend = BackendBuilder::new(listen_addr, vec![(0, peer)], vec![0], 128);
    let op_postbox = backend.for_operator(&0).unwrap();

    let _backend = backend.connect().unwrap();

    op_postbox
        .send(&0, Message::new(0, vec![1u8, 2, 3, 4]))
        .unwrap();

    loop {
        match op_postbox.recv::<Vec<u8>>().unwrap() {
            Some(x) => {
                println!("{x:?}");
                break;
            }
            None => {
                sleep(Duration::from_millis(100));
                continue;
            }
        }
    }
}
