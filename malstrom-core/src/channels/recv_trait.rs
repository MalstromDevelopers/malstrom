/// TODO: do we still need this trait?
pub(crate) trait Receiver {
    type Output;
    async fn recv(&mut self) -> Self::Output;
}