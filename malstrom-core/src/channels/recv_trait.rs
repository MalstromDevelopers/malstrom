use futures::{StreamExt, stream::FuturesUnordered};
use indexmap::IndexMap;

/// TODO: do we still need this trait?
pub(crate) trait Receiver {
    type Output;
    async fn recv(&mut self) -> Self::Output;
}

// impl<K, V> Receiver for IndexMap<K, V> where K: Clone, V: Receiver {
//     type Output = (K, V::Output);

//     /// Receiver impl for a map of receivers, never returns for an empty map
//     async fn recv(&mut self) -> Self::Output {
//         let mut recv_futures: FuturesUnordered<_> = self.iter_mut()
//         .map(|(k, v)| async {(k.clone(), v.recv().await)})
//         .collect();

//         if recv_futures.is_empty() {
//             std::future::pending().await
//         } else {
//             recv_futures.next().await.unwrap()
//         }
//     }
// }
