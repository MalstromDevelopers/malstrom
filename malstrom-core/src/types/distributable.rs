use serde::{Serialize, de::DeserializeOwned};

use crate::types::Kvt;

/// A type which can be sent (distributed) between workers
pub trait Distributable: Serialize + DeserializeOwned + 'static {
    fn encode(self) -> Vec<u8>;

    fn decode(encoded: &[u8]) -> Self;
}
impl<T> Distributable for T
where
    T: Serialize + DeserializeOwned + 'static,
{
    fn encode(self) -> Vec<u8> {
        rmp_serde::encode::to_vec(&self).expect("Encoding error")
    }

    fn decode(encoded: &[u8]) -> Self {
        rmp_serde::decode::from_slice(encoded).expect("Decoding error")
    }
}
