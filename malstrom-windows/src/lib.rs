use std::time::{SystemTime, UNIX_EPOCH};

pub mod sliding;
pub mod processing;
pub mod tumbling;

fn get_processing_time() -> u128 {
    let start = SystemTime::now();
    start
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis()
}
