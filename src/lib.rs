mod channels;
// pub mod filter;
pub mod frontier;
pub mod inspect;
pub mod kafka;
// pub mod map;
pub mod network_exchange;
pub mod snapshot;
pub mod stateful_map;
pub mod stream;
pub mod worker;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum StreamError {
    #[error("Attempted an operation across workers")]
    DifferentWorker,
}
