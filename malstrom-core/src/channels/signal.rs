use futures::FutureExt;
use std::{
    future::Future,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll},
};
use thiserror::Error;
use tokio::sync::{Notify, watch};

pub(crate) struct Signal(watch::Sender<bool>, watch::Receiver<bool>);

impl Signal {
    pub fn new(active: bool) -> Self {
        let (tx, rx) = watch::channel(active);
        Self(tx, rx)
    }

    pub fn handle(&self, name: String) -> SignalHandle {
        SignalHandle(self.1.clone(), name)
    }

    pub fn activate(&self) {
        // PANIC: We hold one receiver ourselves, therefore this is safe to do
        self.0.send(true).expect("Channel must be open")
    }
}

#[derive(Clone)]
pub(crate) struct SignalHandle(watch::Receiver<bool>, String);

impl SignalHandle {
    // wait for this signal to be indicated
    pub async fn watch(mut self) -> Result<(), SignalRecvError> {
        self.0
            .wait_for(|x| *x)
            .await
            .map_err(|_| SignalRecvError::SignalDropped)
            .map(|_| ())
    }
}

#[derive(Debug, Error)]
pub(crate) enum SignalRecvError {
    #[error("The Signal was dropped")]
    SignalDropped,
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
