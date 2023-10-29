pub mod filter;
pub mod map;
pub mod stream;
pub mod worker;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum StreamError {
    #[error("Attempted an operation across workers")]
    DifferentWorker,
}
