use crate::runtime::communication as com;
use async_trait::async_trait;
use flume::{Receiver, Sender};
use tokio::sync::oneshot;

pub(super) struct ReqResSender(Sender<(Vec<u8>, oneshot::Sender<Vec<u8>>)>);

impl ReqResSender {
    pub fn new(sender: Sender<(Vec<u8>, oneshot::Sender<Vec<u8>>)>) -> Self {
        ReqResSender(sender)
    }
}

#[async_trait]
impl com::ReqResSender for ReqResSender {
    async fn send(&self, msg: Vec<u8>) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let (repond_tx, respond_rx) = oneshot::channel();
        self.0
            .send_async((msg, repond_tx))
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error>);
        respond_rx.await.map_err(Into::into)
    }
}

pub(super) struct ReqResReceiver(Receiver<(Vec<u8>, oneshot::Sender<Vec<u8>>)>);

impl ReqResReceiver {
    pub fn new(receiver: Receiver<(Vec<u8>, oneshot::Sender<Vec<u8>>)>) -> Self {
        ReqResReceiver(receiver)
    }
}

#[async_trait]
impl com::ReqResReceiver for ReqResReceiver {
    async fn recv(
        &self,
    ) -> Result<(Vec<u8>, Box<dyn com::ReqResResponder>), Box<dyn std::error::Error>> {
        let (msg, responder) = self.0.recv_async().await.map_err(|e| Box::new(e))?;
        let responder = Box::new(ReqResResponder::new(responder));
        Ok((msg, responder))
    }
}

struct ReqResResponder(Option<oneshot::Sender<Vec<u8>>>);

impl ReqResResponder {
    pub fn new(responder: oneshot::Sender<Vec<u8>>) -> Self {
        ReqResResponder(Some(responder))
    }
}

#[async_trait]
impl com::ReqResResponder for ReqResResponder {
    async fn respond(&mut self, msg: Vec<u8>) -> Result<(), Box<dyn std::error::Error>> {
        /// as per trait implementation note we ignore sends after the first one
        /// see [com::ReqResResponder::respond] doc
        match self.0.take() {
            Some(sender) => {
                let _ = sender.send(msg);
                Ok(())
            }
            None => Ok(()),
        }
    }
}
