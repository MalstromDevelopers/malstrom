use bytes::{Buf, BufMut as _, Bytes, BytesMut};
use flume::{Receiver, Sender};
use malstrom::types::{OperatorId, WorkerId};
use thiserror::Error;
use tonic::Status;

use crate::CONFIG;

pub(super) fn decode_id<T: Buf>(mut raw: T) -> Result<OperatorId, Status> {
    let l = raw.remaining();
    match l {
        8 => Ok(raw.get_u64_le()),
        0..=7 => Err(Status::invalid_argument(format!(
            "ID metadata too short: {l} < 8"
        ))),
        9.. => Err(Status::invalid_argument(format!(
            "ID metadata too long: {l} > 8"
        ))),
    }
}

pub(super) fn encode_id(id: OperatorId) -> Bytes {
    let mut buf = BytesMut::with_capacity(8);
    buf.put_u64_le(id);
    buf.freeze()
}

/// Create a new channel/receiver pair with the configured network buffer capacitiy
pub(super) fn new_channel() -> (Sender<Vec<u8>>, Receiver<Vec<u8>>) {
    flume::bounded(CONFIG.network.buffer_capacity)
}

#[derive(Debug, Error)]
enum GrpcBackendError {
    #[error("Trying to connect two workers of same id: {0}")]
    SameId(WorkerId),
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{BufMut, BytesMut};
    use tonic::Status;

    #[test]
    fn test_decode_operator_id_valid() {
        let mut buf = BytesMut::with_capacity(8);
        buf.put_u64_le(1234_u64); // Example valid operator ID
        let result = decode_id(buf.freeze());
        assert_eq!(result.unwrap(), 1234_u64);
    }

    #[test]
    fn test_decode_operator_id_empty() {
        let buf = BytesMut::new();
        let result = decode_id(buf.freeze());
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().code(),
            Status::invalid_argument("").code()
        );
    }

    #[test]
    fn test_decode_operator_id_too_short() {
        let mut buf = BytesMut::with_capacity(7);
        buf.put(&vec![1, 2, 3, 4, 5, 6, 7][..]); // 7 bytes
        let result = decode_id(buf.freeze());
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().code(),
            Status::invalid_argument("").code()
        );
    }

    #[test]
    fn test_decode_operator_id_too_long() {
        let mut buf = BytesMut::with_capacity(9);
        buf.put(&vec![1, 2, 3, 4, 5, 6, 7, 8, 9][..]); // 9 bytes
        let result = decode_id(buf.freeze());
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().code(),
            Status::invalid_argument("").code()
        );
    }

    #[test]
    fn test_operator_id_round_trip() {
        let original_id: OperatorId = 12345u64;

        let encoded = encode_id(original_id);

        let decoded_result = decode_id(encoded);

        let decoded_id = decoded_result.unwrap();
        assert_eq!(original_id, decoded_id);
    }
}
