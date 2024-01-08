#[derive(Clone, Copy)]
pub enum BarrierData<T, P> {
    Barrier(P),
    Data(T),
    Load(P)
}
