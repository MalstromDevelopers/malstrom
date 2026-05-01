//! Traits for implementing inter-worker and worker-coordinator communication in different runtimes
use std::{marker::PhantomData, rc::Rc};

use async_trait::async_trait;
use serde::{Serialize, de::DeserializeOwned};
use thiserror::Error;
use tracing::debug;

use crate::types::{Kvt, OperatorId, WorkerId, distributable::Distributable};

mod operator_operator;
mod reqres;
mod stream;
mod worker_coordinator;

pub use operator_operator::OperatorOperatorComm;
pub use reqres::{ReqResReceiver, ReqResResponder, ReqResSender};
pub use stream::{StreamReceiver, StreamSender};
pub use worker_coordinator::WorkerCoordinatorComm;

pub(crate) use operator_operator::{OperatorCommReceiver, OperatorCommSender};
pub(crate) use worker_coordinator::{CoordinatorClient, WorkerClient, WorkerResponder};

/// A convinience method to broadcast a message to all available clients
pub async fn broadcast<'a, T: Distributable + Clone + 'a>(
    clients: impl Iterator<Item = &'a OperatorCommSender<T>>,
    msg: T,
) {
    futures::future::join_all(clients.map(|c| c.send(msg.clone()))).await;
}
