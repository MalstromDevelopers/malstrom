//! A very simple implementation of a Send/Sync map where you can register
//! conditions to be evaluated on every change

use std::sync::Arc;

use indexmap::IndexMap;
use std::hash::Hash;
use tokio::sync::{oneshot, Mutex};

/// A Send + Sync map which evaluates given conditions upon every change
#[derive(Default, Clone)]
pub(crate) struct WatchMap<K: Send + Sync, V: Send + Sync> {
    inner: Arc<Mutex<WatchMapInner<K, V>>>,
}

/// [WatchMap] inner type
#[derive(Default)]
struct WatchMapInner<K, V> {
    /// stored data
    store: IndexMap<K, V>,
    /// Conditions to be evaluated on changes and notified if satisfied
    notifications: Vec<Notification<K, V>>,
}

impl<K, V> WatchMapInner<K, V>
where
    K: 'static,
    V: 'static,
{
    /// Check all notifications and fire thos with satisfied conditions
    fn fire_notifications(&mut self) {
        self.notifications = self
            .notifications
            .drain(..)
            .filter_map(|x| x.check(&mut ConditionIter(self.store.iter())))
            .collect();
    }
}

impl<K, V> WatchMap<K, V>
where
    K: Hash + Eq + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    /// Insert a value, returning the previous value if it was present
    pub(crate) async fn insert(&self, key: K, value: V) -> Option<V> {
        let mut guard = self.inner.lock().await;
        let prev = guard.store.insert(key, value);
        guard.fire_notifications();
        prev
    }

    /// notify once a condition is satisfied
    pub(crate) async fn notify(&self, condition: impl Condition<K, V>) -> oneshot::Receiver<()> {
        let (notification, on_complete) = Notification::new(condition);
        self.inner.lock().await.notifications.push(notification);
        on_complete
    }

    /// Remove a key, possibly returning it's value if it was in the map
    pub(crate) async fn remove(&self, key: &K) -> Option<V> {
        let mut guard = self.inner.lock().await;
        let value = guard.store.swap_remove(key);
        guard.fire_notifications();
        value
    }
}

impl<K, V> WatchMap<K, V>
where
    K: Hash + Eq + Send + Sync + 'static,
    V: Default + Send + Sync + 'static,
{
    /// Applies a function to the value associated with the specified key, or inserts a default
    /// value and applies the function if the key is not already present.
    /// # Arguments
    ///
    /// - `key` - The key associated with the value to be applied or defaulted.
    /// - `func` - A function that is applied to the value if the key exists, or to the default
    ///            value if the key does not exist. It takes a mutable reference to the value.
    ///
    /// After applying the function or inserting a default value, it triggers any pending
    /// notifications that depend on changes to the map.
    pub(crate) async fn apply_or_default<F>(&self, key: K, func: F)
    where
        F: FnOnce(&mut V),
    {
        let mut guard = self.inner.lock().await;
        let value = guard.store.entry(key).or_default();
        func(value);
        guard.fire_notifications();
    }
}

impl<K, V> WatchMap<K, V>
where
    K: Clone + Send + Sync,
    V: Clone + Send + Sync,
{
    /// Clone the contained inner data store
    pub(crate) async fn clone_inner_map(&self) -> IndexMap<K, V> {
        self.inner.lock().await.store.clone()
    }
}

impl<K, V> From<IndexMap<K, V>> for WatchMap<K, V>
where
    K: Send + Sync,
    V: Send + Sync,
{
    /// Create a [WatchMap] from an [IndexMap]. The created map will have no notifications
    /// registered.
    fn from(value: IndexMap<K, V>) -> Self {
        let inner = WatchMapInner {
            store: value,
            notifications: Vec::new(),
        };
        WatchMap {
            inner: Arc::new(Mutex::new(inner)),
        }
    }
}

/// A Notification consisting of a condition to evaluate and a oneshot to complete once
/// the condition has been satisfied
struct Notification<K, V> {
    condition: Box<dyn Condition<K, V>>,
    on_complete: oneshot::Sender<()>,
}
impl<K, V> Notification<K, V>
where
    K: 'static,
    V: 'static,
{
    /// Create a new [Notification]
    ///
    /// # Returns
    /// - Self: The Notification
    /// - [oneshot::Receiver]: Resolves once condition is satisfied
    fn new(condition: impl Condition<K, V>) -> (Self, oneshot::Receiver<()>) {
        let (tx, rx) = oneshot::channel();
        let notification = Self {
            condition: Box::new(condition),
            on_complete: tx,
        };
        (notification, rx)
    }

    /// Check if condition is satisfied and fire notification if true.
    /// Firing the notification destroys it, hence this returns `Option<Self>`
    fn check(self, values: &mut ConditionIter<K, V>) -> Option<Self> {
        if self.condition.evaluate(values) {
            // ignoring the error here is fine. Simply means the receiver
            // was dropped
            let _ = self.on_complete.send(());
            None
        } else {
            Some(self)
        }
    }
}

pub trait Condition<K, V>: Send + Sync + 'static {
    /// Watch changes to a key, once this returns true the watch will be ended
    fn evaluate(&self, items: &mut ConditionIter<K, V>) -> bool;
}
impl<X, K, V> Condition<K, V> for X
where
    X: Fn(&mut ConditionIter<K, V>) -> bool + Send + Sync + 'static,
{
    fn evaluate(&self, items: &mut ConditionIter<K, V>) -> bool {
        self(items)
    }
}

pub(super) struct ConditionIter<'a, K, V>(indexmap::map::Iter<'a, K, V>);
impl<'a, K, V> Iterator for ConditionIter<'a, K, V> {
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::timeout;

    use crate::coordinator::watchmap::ConditionIter;

    use super::WatchMap;
    fn is_send_sync<T: Send + Sync>(_: T) {}

    #[test]
    fn test_is_send_sync() {
        let map: WatchMap<usize, usize> = WatchMap::default();
        is_send_sync(map);
    }

    #[tokio::test]
    async fn test_insert() {
        let db = WatchMap::default();
        let value = "foo".to_string();
        let x = db.insert(42, value.clone()).await;
        assert!(x.is_none());

        let x = db.insert(42, "hi".to_string()).await.unwrap();
        assert_eq!(x, value);
    }

    #[tokio::test]
    async fn test_notify_apply() {
        let db = WatchMap::default();
        db.insert(42, 42).await;
        let fut = db.notify(|values: &mut ConditionIter<i32, i32>| values.any(|(_k, v)| *v > 80));
        db.apply_or_default(42, |x| *x *= 2).await;
        timeout(Duration::from_millis(10), fut).await.unwrap();
    }

    #[tokio::test]
    async fn test_notify_insert() {
        let db = WatchMap::default();
        let fut = db.notify(|values: &mut ConditionIter<i32, i32>| values.any(|(_k, v)| *v == 42));
        db.insert(42, 42).await;
        timeout(Duration::from_millis(10), fut).await.unwrap();
    }

    #[tokio::test]
    async fn test_notify_remove() {
        let db = WatchMap::default();
        db.insert(42, 42).await;
        let fut = db.notify(|values: &mut ConditionIter<i32, i32>| values.all(|_| false));
        db.remove(&42).await;
        timeout(Duration::from_millis(10), fut).await.unwrap();
    }
}
