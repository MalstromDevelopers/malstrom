use crate::runtime::communication as com;
use async_trait::async_trait;
use flume::{Receiver, Sender};
use tokio::sync::oneshot;

pub(super) struct OperatorSender(Sender<Vec<u8>>);

impl OperatorSender {
    pub fn new(sender: Sender<Vec<u8>>) -> Self {
        OperatorSender(sender)
    }
}

#[async_trait]
impl com::StreamSender for OperatorSender {
    async fn send(&self, msg: Vec<u8>) -> Result<(), Box<dyn std::error::Error>> {
        self.0.send_async(msg).await.map_err(|e| e.into())
    }
}

pub(super) struct OperatorReceiver(Receiver<Vec<u8>>);

impl OperatorReceiver {
    pub fn new(receiver: Receiver<Vec<u8>>) -> Self {
        OperatorReceiver(receiver)
    }
}

#[async_trait]
impl com::StreamReceiver for OperatorReceiver {
    async fn recv(&self) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        self.0.recv_async().await.map_err(|e| e.into())
    }
}
