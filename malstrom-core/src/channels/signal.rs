use futures::FutureExt;
use std::{
    future::Future,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll},
};
use tokio::sync::Notify;

pub(crate) struct Signal(Rc<Notify>);

impl Signal {
    pub fn new() -> Self {
        Self(Rc::new(Notify::new()))
    }

    pub fn handle(&self) -> SignalHandle {
        SignalHandle(Rc::clone(&self.0))
    }

    pub fn send(&self) {
        self.0.notify_waiters();
    }
}

#[derive(Clone)]
pub(crate) struct SignalHandle(Rc<Notify>);

impl SignalHandle {
    // wait for this signal to be indicated
    pub async fn watch(self) {
        self.0.notified().await
    }
}

// TODO: tests
// #[cfg(test)]
// mod tests {
//     use super::*;
//     use tokio::time::{Duration, timeout};

//     #[tokio::test]
//     async fn test_last_ref_standing_not_complete() {
//         let last_ref = Signal::new();
//         let handle1 = last_ref.handle();
//         let handle2 = last_ref.handle();

//         // Drop one handle, but not the last one
//         drop(handle1);

//         // The future should not complete yet
//         let fut = last_ref.await_last();
//         let result = timeout(Duration::from_millis(100), fut).await;
//         assert!(result.is_err(), "Future should not complete yet");
//     }

//     #[tokio::test]
//     async fn test_last_ref_standing_complete() {
//         let last_ref = Signal::new();
//         let handle1 = last_ref.handle();
//         let handle2 = last_ref.handle();

//         // Drop one handle, but not the last one
//         drop(handle1);

//         // Drop the last handle
//         drop(handle2);

//         // The future should complete now
//         let fut = last_ref.await_last();
//         let result = timeout(Duration::from_millis(100), fut).await;
//         assert!(result.is_ok(), "Future should complete now");
//     }

//     #[tokio::test]
//     async fn test_no_handle_created() {
//         let last_ref = Signal::new();

//         // The future should complete immediately since no handles are created
//         let fut = last_ref.await_last();
//         let result = timeout(Duration::from_millis(100), fut).await;
//         assert!(result.is_ok(), "Future should complete immediately");
//     }
// }
