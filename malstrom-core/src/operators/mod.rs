//! Operators for performing various operations on data in a job
// public API operators
mod cloned;
mod filter;
mod filter_map;
mod flatten;
mod inspect;
mod map;
mod ttl_map;
mod sink;
mod source;
mod split;
mod stateful_map;
mod stateful_op;
mod time;

// Public Api operators reexported for convenience
pub use crate::keyed::KeyDistribute;
pub use crate::keyed::KeyLocal;
pub use cloned::Cloned;
pub use filter::Filter;
pub use filter_map::FilterMap;
pub use flatten::Flatten;
pub use inspect::Inspect;
pub use map::Map;
pub use ttl_map::{ExpiryEntry, ExpireMap, TtlMap};
pub use sink::{Sink, StreamSink};
pub use source::{Source, StreamSource};
pub use split::Split;
pub use stateful_map::StatefulMap;
pub use stateful_op::{StatefulLogic, StatefulOp};
pub use time::*;

// These are only to be used internally in malstrom
pub(crate) mod stateless_op;

// marker used to seal the traits implementing operators
// on JetStreamBuilder
mod sealed {
    use crate::stream::StreamBuilder;

    use super::NeedsEpochs;
    pub trait Sealed {}

    impl<K, V, T> Sealed for StreamBuilder<K, V, T> {}
    impl<K, V, T> Sealed for NeedsEpochs<K, V, T> {}
}
