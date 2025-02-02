use async_trait::async_trait;
use futures::{stream::FuturesUnordered, StreamExt};

#[async_trait]
pub(crate) trait FailFast<T> {
    async fn failfast(self) -> Option<Result<T, tokio::task::JoinError>>;
}

#[async_trait]
impl<T> FailFast<T> for FuturesUnordered<tokio::task::JoinHandle<T>>
where
    T: Send,
{
    async fn failfast(mut self) -> Option<Result<T, tokio::task::JoinError>> {
        let first = self.next().await;
        for t in self.into_iter() {
            t.abort();
            let _ = t.await;
        }
        first
    }
}
