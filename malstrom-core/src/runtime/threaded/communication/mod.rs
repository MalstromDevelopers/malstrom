use crate::{
    runtime::{OperatorOperatorComm, communication as com},
    types::{OperatorId, WorkerId},
};
use async_trait::async_trait;
use flume::{Receiver, Sender};
use std::sync::{Arc, Mutex};
use thiserror::Error;
use tokio::sync::oneshot;

use indexmap::IndexMap;
use tracing::debug;

mod inter_thread;
mod reqres;
mod stream;

pub(super) use inter_thread::{
    CoordinatorChannels, CoordinatorCommunication, OperatorChannels, OperatorCommunication,
};
use reqres::{ReqResReceiver, ReqResSender};
use stream::{OperatorReceiver, OperatorSender};

/// uniquely identifies a connection
#[derive(Debug, Hash, PartialEq, Eq, Clone, Copy)]
pub(super) struct ConnectionKey {
    sending: WorkerId,
    receiving: WorkerId,
    operator: OperatorId,
}
impl ConnectionKey {
    /// generates the same key no matter in which direction the connection is supplied
    fn new(sending: WorkerId, receiving: WorkerId, operator: OperatorId) -> Self {
        Self {
            sending,
            receiving,
            operator,
        }
    }
}
