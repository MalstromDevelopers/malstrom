mod channels;
pub mod filter;
pub mod frontier;
pub mod inspect;
pub mod kafka;
pub mod map;
pub mod network_exchange;
pub mod stream;
pub mod worker;
pub mod snapshot;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum StreamError {
    #[error("Attempted an operation across workers")]
    DifferentWorker,
}
