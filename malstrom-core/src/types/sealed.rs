// marker used to seal the traits implementing operators
pub(super) mod sealed {
    use crate::{
        stream::{InitialStreamBuilder, StreamBuilder},
        types::Kvt,
    };

    // use super::NeedsEpochs;
    pub trait Sealed {}

    impl<M: Kvt> Sealed for StreamBuilder<M> {}
    impl Sealed for InitialStreamBuilder {}
    // impl<K, V, T> Sealed for NeedsEpochs<K, V, T> {}
}
