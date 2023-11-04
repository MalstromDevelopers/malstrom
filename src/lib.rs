pub mod filter;
pub mod map;
pub mod stream;
pub mod worker;
pub mod frontier;
mod watch;
// pub mod network_exchange;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum StreamError {
    #[error("Attempted an operation across workers")]
    DifferentWorker,
}
