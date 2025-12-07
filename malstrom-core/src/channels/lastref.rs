use std::{
    future::Future,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll},
};
use tokio::sync::Notify;

/// A reference counted type which allows one reference holder
/// to wait until it is holding the last reference
pub(crate) struct LastRefStanding(Rc<Notify>);

impl LastRefStanding {
    pub fn new() -> Self {
        Self(Rc::new(Notify::new()))
    }

    pub fn handle(&self) -> LastRefHandle {
        LastRefHandle(Rc::clone(&self.0))
    }

    /// Await on this future to wait until this is the last reference
    /// Takes ownership of self to avoid endless waits due to multiple
    /// outstanding awaits
    pub async fn await_last(self) -> Self {
        if Rc::strong_count(&self.0) == 1 {
            return self;
        }
        self.0.notified().await;
        self
    }
}

#[derive(Clone)]
pub(crate) struct LastRefHandle(Rc<Notify>);

impl Drop for LastRefHandle {
    fn drop(&mut self) {
        // If this is the last reference, notify all waiters.
        if Rc::strong_count(&self.0) <= 1 {
            self.0.notify_waiters();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{Duration, timeout};

    #[tokio::test]
    async fn test_last_ref_standing_not_complete() {
        let last_ref = LastRefStanding::new();
        let handle1 = last_ref.handle();
        let handle2 = last_ref.handle();

        // Drop one handle, but not the last one
        drop(handle1);

        // The future should not complete yet
        let fut = last_ref.await_last();
        let result = timeout(Duration::from_millis(100), fut).await;
        assert!(result.is_err(), "Future should not complete yet");
    }

    #[tokio::test]
    async fn test_last_ref_standing_complete() {
        let last_ref = LastRefStanding::new();
        let handle1 = last_ref.handle();
        let handle2 = last_ref.handle();

        // Drop one handle, but not the last one
        drop(handle1);

        // Drop the last handle
        drop(handle2);

        // The future should complete now
        let fut = last_ref.await_last();
        let result = timeout(Duration::from_millis(100), fut).await;
        assert!(result.is_ok(), "Future should complete now");
    }

    #[tokio::test]
    async fn test_no_handle_created() {
        let last_ref = LastRefStanding::new();

        // The future should complete immediately since no handles are created
        let fut = last_ref.await_last();
        let result = timeout(Duration::from_millis(100), fut).await;
        assert!(result.is_ok(), "Future should complete immediately");
    }
}
