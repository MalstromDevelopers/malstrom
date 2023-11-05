pub mod filter;
pub mod frontier;
pub mod map;
pub mod state_backend;
pub mod stream;
mod watch;
pub mod worker;
pub mod network_exchange;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum StreamError {
    #[error("Attempted an operation across workers")]
    DifferentWorker,
}
