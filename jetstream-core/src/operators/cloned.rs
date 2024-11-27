use crate::{stream::JetStreamBuilder, types::{MaybeData, MaybeKey, MaybeTime}};

use super::split::Split;

pub trait Cloned<K, V, T> {
    /// Create N new streams by copying all messages into every created stream.
    /// To partition the stream instead see [super::split::Split].
    fn const_cloned<const N: usize>(
        self,
    ) -> [JetStreamBuilder<K, V, T>; N];

    /// Create N new streams by copying all messages into every created stream.
    /// To partition the stream instead see [super::split::Split].
    fn cloned( self, outputs: usize ) -> Vec<JetStreamBuilder<K, V, T>>;
}

impl<K, V, T> Cloned<K, V, T> for JetStreamBuilder<K, V, T> where 
K: MaybeKey,
V: MaybeData,
T: MaybeTime {
    fn const_cloned<const N: usize>(
        self,
    ) -> [JetStreamBuilder<K, V, T>; N] {
        self.const_split(|_, i| (0..i).collect())
    }

    fn cloned( self, outputs: usize ) -> Vec<JetStreamBuilder<K, V, T>> {
        self.split(|_, i| (0..i).collect(), outputs)
    }
}