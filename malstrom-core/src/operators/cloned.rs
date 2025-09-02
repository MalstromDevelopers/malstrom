use crate::{
    stream::StreamBuilder,
    types::{MaybeData, MaybeKey, MaybeTime},
};

use super::split::Split;

/// Create multiple streams by cloning the message from a single stream.
pub trait Cloned<K, V, T>: super::sealed::Sealed {
    /// Create N new streams by copying all messages into every created stream.
    /// To partition the stream instead see [super::split::Split::const_split].
    fn const_cloned<const N: usize>(self, name: &str) -> [StreamBuilder<K, V, T>; N];

    /// Create N new streams by copying all messages into every created stream.
    /// To partition the stream instead see [super::split::Split::split].
    fn cloned(self, name: &str, outputs: usize) -> Vec<StreamBuilder<K, V, T>>;
}

impl<K, V, T> Cloned<K, V, T> for StreamBuilder<K, V, T>
where
    K: MaybeKey,
    V: MaybeData,
    T: MaybeTime,
{
    fn const_cloned<const N: usize>(self, name: &str) -> [StreamBuilder<K, V, T>; N] {
        self.const_split(name, |_, outputs: &mut [bool; N]| {
            *outputs = [true; N];
        })
    }

    fn cloned(self, name: &str, outputs: usize) -> Vec<StreamBuilder<K, V, T>> {
        self.split(name, |_, outs: &mut [bool]| outs.fill(true), outputs)
    }
}
