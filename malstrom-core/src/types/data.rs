//! Types and traits for data processed in JetStream

/// Data which may move through a stream
#[diagnostic::on_unimplemented(message = "Type must be `Clone + 'static` to be used as data")]
pub trait Data: Clone + 'static {}
impl<T: Clone + 'static> Data for T {}

/// Marker trait to denote streams that may or may not have data
pub trait MaybeData: Clone + 'static {}
impl<T: Clone + 'static> MaybeData for T {}

/// Zero sized indicator for a stream with no data
#[derive(Clone, Debug)]
pub struct NoData;
