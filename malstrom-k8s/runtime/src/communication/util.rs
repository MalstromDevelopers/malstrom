use flume::{Receiver, Sender};
use tonic::Status;

use crate::CONFIG;

#[allow(clippy::result_large_err)]
pub(super) fn decode_id(raw: &str) -> Result<u64, Status> {
    raw.parse::<u64>()
        .map_err(|e| Status::invalid_argument(format!("Invalid u64: {e:?}")))
}

/// Create a new channel/receiver pair with the configured network buffer capacitiy
pub(super) fn new_channel() -> (Sender<Vec<u8>>, Receiver<Vec<u8>>) {
    flume::bounded(CONFIG.network.buffer_capacity)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::Status;

    #[test]
    fn test_decode_operator_id_valid() {
        let result = decode_id("1234");
        assert_eq!(result.unwrap(), 1234_u64);
    }

    #[test]
    fn test_decode_operator_id_empty() {
        let result = decode_id("");
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().code(),
            Status::invalid_argument("").code()
        );
    }
}
