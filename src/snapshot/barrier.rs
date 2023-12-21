#[derive(Clone, Copy)]
pub enum BarrierData<T> {
    Barrier(usize),
    Data(T),
}
