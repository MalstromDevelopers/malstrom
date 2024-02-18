pub mod selective_broadcast;
pub mod watch;

#[derive(Clone)]
pub enum BarrierData<T, P> {
    /// normal data
    Data(T),
    // ABS Barrier
    Barrier(P),
    /// Snapshot Load
    Load(P),
}
