use timely::communication::allocator::Generic;
use timely::dataflow::scopes::Child;
use timely::dataflow::Stream;
use timely::worker::Worker as TimelyWorker;
use timely::Data;

pub mod persistent;
mod split;
mod util;
mod union;
mod sketch;

/// Signifies an empty stream
#[derive(Clone)]
struct Empty(());

/// A union type which is used as a container when unioning two streams
#[derive(Clone, Debug)]
pub enum DataUnion<L, R> {
    Left(L),
    Right(R)
}


/// A Stream which produces data of type D
pub struct JetStream<'scp, D>
where
    D: Data,
{
    // TODO: we are hardcoding all timestamps to be u64 or bound
    // to `u64: Refines<T>` here.
    // This comes at the cost of flexibility but otherwise we would
    // spend a lot of time battling generics and type contraints.
    // To be revisited in the future
    scope: &'scp mut Child<'scp, TimelyWorker<Generic>, u64>,
    stream: Stream<Child<'scp, TimelyWorker<Generic>, u64>, D>,
}

impl<'scp> JetStream<'scp, Empty>
{
    /// Create a new empty stream.
    /// This stream does nothing, it reads not data
    /// and performs no operations.
    /// Call other methods like `input` on this stream
    /// to make it useful
    pub fn new(scope: &'scp mut Child<'scp, TimelyWorker<Generic>, u64>) -> Self {
        
        // Hack, this justs gets the dataflow to start
        let stream = util::once(scope, Empty(()));
        return JetStream {
            scope,
            stream,
        };
    }
}
