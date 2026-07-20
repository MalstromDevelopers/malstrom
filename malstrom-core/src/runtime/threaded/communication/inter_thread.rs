use crate::{
    runtime::{
        OperatorOperatorComm,
        communication::{self as com, StreamReceiver, StreamSender},
        threaded::communication::{
            ConnectionKey, OperatorReceiver, OperatorSender, ReqResReceiver, ReqResSender,
        },
    },
    types::{OperatorId, WorkerId},
};
use async_trait::async_trait;
use flume::{Receiver, Sender};
use std::sync::{Arc, Mutex};
use thiserror::Error;
use tokio::sync::oneshot;

use indexmap::IndexMap;
use tracing::debug;

type SenderMap<T> = IndexMap<ConnectionKey, Sender<T>>;
type ReceiverMap<T> = IndexMap<ConnectionKey, Receiver<T>>;
/// AddressMap shared across multiple threads
pub(crate) type InterThreadChannels<T> = Arc<Mutex<(SenderMap<T>, ReceiverMap<T>)>>;

pub(crate) type OperatorChannels = InterThreadChannels<Vec<u8>>;
pub(crate) type OperatorCommunication = InterThreadCommunication<Vec<u8>>;

pub(crate) type CoordinatorChannels = InterThreadChannels<(Vec<u8>, oneshot::Sender<Vec<u8>>)>;
pub(crate) type CoordinatorCommunication =
    InterThreadCommunication<(Vec<u8>, oneshot::Sender<Vec<u8>>)>;

/// Provides simple inter-thread communication via channels
pub struct InterThreadCommunication<T> {
    channels: InterThreadChannels<T>,
    this_worker: WorkerId,
}
impl<T> InterThreadCommunication<T> {
    pub(crate) fn new(channels: InterThreadChannels<T>, this_worker: WorkerId) -> Self {
        Self {
            channels,
            this_worker,
        }
    }

    fn get_or_create_sender(&self, key: ConnectionKey) -> Sender<T> {
        let mut channels = self.channels.lock().unwrap();
        match channels.0.get(&key) {
            Some(tx) => tx.clone(),
            None => {
                let (tx, rx) = flume::bounded(1024);
                channels.0.insert(key.clone(), tx.clone());
                channels.1.insert(key.clone(), rx);
                tx
            }
        }
    }

    fn get_or_create_receiver(&self, key: ConnectionKey) -> Receiver<T> {
        let mut channels = self.channels.lock().unwrap();
        match channels.1.get(&key) {
            Some(rx) => rx.clone(),
            None => {
                let (tx, rx) = flume::bounded(1024);
                channels.0.insert(key.clone(), tx);
                channels.1.insert(key.clone(), rx.clone());
                rx
            }
        }
    }
}

#[derive(Debug, Error)]
enum InterThreadCommunicationError {}

#[async_trait]
impl OperatorOperatorComm for InterThreadCommunication<Vec<u8>> {
    async fn new_sender(
        &self,
        to_worker: WorkerId,
        to_operator: OperatorId,
    ) -> Result<Box<dyn StreamSender>, Box<dyn std::error::Error>> {
        let key = ConnectionKey::new(self.this_worker, to_worker, to_operator);
        let sender = self.get_or_create_sender(key);
        Ok(Box::new(OperatorSender::new(sender)))
    }

    async fn new_receiver(
        &self,
        from_worker: WorkerId,
        from_operator: OperatorId,
    ) -> Result<Box<dyn StreamReceiver>, Box<dyn std::error::Error>> {
        let key = ConnectionKey::new(from_worker, self.this_worker, from_operator);
        let receiver = self.get_or_create_receiver(key);
        Ok(Box::new(OperatorReceiver::new(receiver)))
    }
}

impl com::WorkerCoordinatorComm for InterThreadCommunication<(Vec<u8>, oneshot::Sender<Vec<u8>>)> {
    async fn worker_to_coordinator(&self) -> Result<ReqResReceiver, Box<dyn std::error::Error>> {
        let key = ConnectionKey::new(self.this_worker, WorkerId::MAX, 0);
        let rx = self.get_or_create_receiver(key);
        Ok(ReqResReceiver::new(rx))
    }

    async fn coordinator_to_worker(
        &self,
        to_worker: WorkerId,
    ) -> Result<ReqResSender, Box<dyn std::error::Error>> {
        let key = ConnectionKey::new(WorkerId::MAX, to_worker, 0);
        let tx = self.get_or_create_sender(key);
        Ok(ReqResSender::new(tx))
    }
}
