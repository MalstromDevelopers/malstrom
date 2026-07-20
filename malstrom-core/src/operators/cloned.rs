use crate::{
    stream::StreamBuilder,
    types::{Kvt, MaybeData, MaybeKey, MaybeTime, Sealed},
};

use super::split::Split;

/// Create multiple streams by cloning the message from a single stream.
pub trait Cloned<In: Kvt>: Sealed {
    /// Create N new streams by copying all messages into every created stream.
    /// To partition the stream instead see [super::split::Split::const_split].
    fn const_cloned<const N: usize>(self, name: &str) -> [StreamBuilder<In>; N];

    /// Create N new streams by copying all messages into every created stream.
    /// To partition the stream instead see [super::split::Split::split].
    fn cloned(self, name: &str, outputs: usize) -> Vec<StreamBuilder<In>>;
}

impl<In> Cloned<In> for StreamBuilder<In>
where
    In: Kvt,
{
    fn const_cloned<const N: usize>(self, name: &str) -> [StreamBuilder<In>; N] {
        self.const_split(name, |_, outputs: &mut [bool; N]| {
            *outputs = [true; N];
        })
    }

    fn cloned(self, name: &str, outputs: usize) -> Vec<StreamBuilder<In>> {
        self.split(name, |_, outs: &mut [bool]| outs.fill(true), outputs)
    }
}
