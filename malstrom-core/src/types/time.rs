//! Types and traits specific to time-keeping and timestamped streams.

use serde::{Deserialize, Serialize};

/// Trait implemented by all types usable as timestamps in JetStream
pub trait Timestamp: PartialOrd + Ord + Clone + std::fmt::Debug + 'static {
    /// Maximum or final value of this type. This is the last possible timestamp.
    const MAX: Self;
    /// Minumum value of this type.
    const MIN: Self;

    /// Merges two timestamps. Merging is used to align timestamps coming from
    /// multiple source e.g. in keyed streams. Merging should yield the lowest
    /// common timestamp of the two values. For most types this will be equivalent
    /// to the minimum of the two values.
    fn merge(&self, other: &Self) -> Self;
}

/// Zero sozed marker indicating a stream with no timestamps associated.
///
/// **IMPORTANT:** The NoTime type has a special meaning in JetStream:
/// Operators emittng `NoTime` are seen as not able to advance the computation.
/// This means if all operators emitting timestamps in a stream are finished, a `NoTime`
/// emitting operator will not keep the stream running.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NoTime;

impl PartialOrd for NoTime {
    fn partial_cmp(&self, _other: &Self) -> Option<std::cmp::Ordering> {
        None
    }
}

/// Time where the timestamp may not yet have been set
pub trait MaybeTime: std::fmt::Debug + Clone + PartialOrd + 'static {
    /// Try to merge two times, returning Some if the
    /// specific type implementing this trait implements
    /// Timestamp and None if it does not
    fn try_merge(&self, other: &Self) -> Option<Self>;

    /// Check if an optional timestamp is equal to the max timestamp
    /// This absolutely cursed implementation of a static function allows
    /// us to indicate that NoTime emitting operators are always done
    const CHECK_FINISHED: fn(&Option<Self>) -> bool;
}
impl<T> MaybeTime for T
where
    T: Timestamp + Clone + 'static,
{
    fn try_merge(&self, other: &Self) -> Option<Self> {
        Some(self.merge(other))
    }

    const CHECK_FINISHED: fn(&Option<Self>) -> bool =
        |opt_t| opt_t.as_ref().is_some_and(|t| *t == T::MAX);
}
impl MaybeTime for NoTime {
    fn try_merge(&self, _other: &Self) -> Option<Self> {
        Some(NoTime)
    }
    /// Always true, as a `NoTime` emitting Operator can not keep a stream running.
    const CHECK_FINISHED: fn(&Option<Self>) -> bool = |_| true;
}

/// Implements `Timestamp` for numeric types
macro_rules! timestamp_impl {
    ($t:ty) => {
        impl Timestamp for $t {
            const MAX: $t = <$t>::MAX;
            const MIN: $t = <$t>::MIN;

            fn merge(&self, other: &$t) -> $t {
                *self.min(other)
            }
        }
    };
}

timestamp_impl!(usize);
timestamp_impl!(u8);
timestamp_impl!(u16);
timestamp_impl!(u32);
timestamp_impl!(u64);
timestamp_impl!(u128);

timestamp_impl!(isize);
timestamp_impl!(i8);
timestamp_impl!(i16);
timestamp_impl!(i32);
timestamp_impl!(i64);
timestamp_impl!(i128);
