use std::time::Duration;

use bon::Builder;
use communication::{APICommand, WorkerGrpcBackend};
use config::CONFIG;
use malstrom::{
    coordinator::{Coordinator, CoordinatorExecutionError},
    runtime::{CommunicationError, RuntimeFlavor},
    snapshot::PersistenceBackend,
    types::WorkerId,
    worker::{StreamProvider, WorkerBuilder, WorkerExecutionError},
};
use thiserror::Error;
mod communication;
mod config;
use crate::communication::CoordinatorGrpcBackend;

#[derive(Builder)]
pub struct KubernetesRuntime<P> {
    #[builder(finish_fn)]
    build: fn(&mut dyn StreamProvider) -> (),
    persistence: P,
    snapshots: Option<Duration>,
}

impl<P> KubernetesRuntime<P>
where
    P: PersistenceBackend + Clone + Send + Sync,
{
    /// Execute as a worker or coordinator depending on environment settings.
    /// When used with the Malstrom Kubernetes Operator this will automatically execute as the
    /// correct role.
    pub fn execute_auto(self) -> Result<(), ExecuteAutoError> {
        if CONFIG.is_coordinator {
            self.execute_coordinator()?;
        } else {
            self.execute_worker()?;
        };
        Ok(())
    }

    /// Execute job as a worker
    pub fn execute_worker(self) -> Result<(), WorkerExecutionError> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads((CONFIG.initial_scale * 2) as usize)
            .enable_all()
            .build()
            .unwrap();
        let mut worker = WorkerBuilder::new(
            KubernetesRuntimeFlavor(rt.handle().clone()),
            self.persistence,
        );
        (self.build)(&mut worker);
        worker.execute()
    }

    /// Execute job as a coordinator
    pub fn execute_coordinator(self) -> Result<(), CoordinatorExecutionError> {
        // channel to connect the GRPC server API to the coordinator API
        let rt = tokio::runtime::Builder::new_multi_thread()
            // 4 = best number. No really, 1 leads to issues. Our communication workload
            // is usually small, 4 seems good.
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap();
        // channel to send API commands (e.g. rescale to the inner coordinator)
        let (api_tx, api_rx) = flume::unbounded();

        let communication = CoordinatorGrpcBackend::new(api_tx, rt.handle().clone()).unwrap();
        let (coordinator, coordinator_api) = Coordinator::new();

        rt.spawn_blocking(move || {
            coordinator
                .execute(
                    CONFIG.initial_scale,
                    self.snapshots,
                    self.persistence,
                    communication,
                )
                .unwrap()
        });

        let api_thread = rt.spawn(async move {
            while let Ok(req) = api_rx.recv() {
                match req {
                    APICommand::Rescale(rescale_command) => {
                        // TODO: Error handling
                        coordinator_api
                            .rescale(rescale_command.desired)
                            .await
                            .unwrap();
                        let _ = rescale_command.on_finish.send(());
                    }
                }
            }
        });

        rt.block_on(api_thread).unwrap();

        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum ExecuteAutoError {
    #[error(transparent)]
    WorkerBuild(#[from] WorkerExecutionError),
    #[error(transparent)]
    CoordinatorCreate(#[from] CoordinatorExecutionError),
}

pub struct KubernetesRuntimeFlavor(tokio::runtime::Handle);

impl RuntimeFlavor for KubernetesRuntimeFlavor {
    type Communication = WorkerGrpcBackend;

    fn communication(&mut self) -> Result<Self::Communication, CommunicationError> {
        WorkerGrpcBackend::new(self.0.clone()).map_err(CommunicationError::from_error)
    }

    fn this_worker_id(&self) -> WorkerId {
        crate::config::CONFIG.get_worker_id()
    }
}

#[cfg(test)]
mod tests {
    use crate::communication::transport::GrpcTransport;

    use super::*;
    use malstrom::runtime::communication::BiStreamTransport;
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::transport::Endpoint;

    async fn get_listener_stream() -> (TcpListenerStream, Endpoint) {
        // see https://stackoverflow.com/a/71808401
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let stream = TcpListenerStream::new(listener);
        let endpoint = Endpoint::try_from(format!("http://{addr}")).unwrap();
        (stream, endpoint)
    }

    /// We should be able to send a message from the coordinator to the worker
    #[tokio::test]
    async fn test_message_coordinator_to_worker() {
        let rt = tokio::runtime::Handle::current();

        let (coord_incoming, coord_endpoint) = get_listener_stream().await;
        let (worker_incoming, worker_endpoint) = get_listener_stream().await;

        let (command_tx, _command_rx) = flume::unbounded();
        let coord_backend =
            CoordinatorGrpcBackend::new_with_incoming(rt.clone(), command_tx, coord_incoming)
                .unwrap();
        let worker_backend =
            WorkerGrpcBackend::new_with_incoming(rt.clone(), worker_incoming).unwrap();

        let coord_transport = GrpcTransport::coordinator_worker(&coord_backend, 0, worker_endpoint);
        let worker_transport = GrpcTransport::worker_coordinator(&worker_backend, coord_endpoint);

        let msg = vec![0, 0, 8, 1, 2];
        coord_transport.send(msg.clone()).unwrap();
        let received = worker_transport.recv_async().await.unwrap();

        assert_eq!(msg, received)
    }

    /// We should be able to send a message from the worker to the coordinator
    #[tokio::test]
    async fn test_message_worker_to_coordinator() {
        let rt = tokio::runtime::Handle::current();

        let (coord_incoming, coord_endpoint) = get_listener_stream().await;
        let (worker_incoming, worker_endpoint) = get_listener_stream().await;

        let (command_tx, _command_rx) = flume::unbounded();
        let coord_backend =
            CoordinatorGrpcBackend::new_with_incoming(rt.clone(), command_tx, coord_incoming)
                .unwrap();
        let worker_backend =
            WorkerGrpcBackend::new_with_incoming(rt.clone(), worker_incoming).unwrap();

        let coord_transport = GrpcTransport::coordinator_worker(&coord_backend, 0, worker_endpoint);
        let worker_transport = GrpcTransport::worker_coordinator(&worker_backend, coord_endpoint);

        let msg = vec![5, 5, 8, 1, 2];
        worker_transport.send(msg.clone()).unwrap();
        let received = coord_transport.recv_async().await.unwrap();

        assert_eq!(msg, received)
    }

    /// We should be able to send a message, even when the worker is not connected yet
    #[tokio::test]
    async fn test_message_coordinator_to_unconnected() {
        let rt = tokio::runtime::Handle::current();

        let (coord_incoming, _) = get_listener_stream().await;

        let (command_tx, _command_rx) = flume::unbounded();
        let coord_backend =
            CoordinatorGrpcBackend::new_with_incoming(rt.clone(), command_tx, coord_incoming)
                .unwrap();

        let fake_endpoint = Endpoint::try_from(format!("http://127.0.0.1:99999")).unwrap();
        let coord_transport = GrpcTransport::coordinator_worker(&coord_backend, 0, fake_endpoint);
        let msg = vec![5, 5, 8, 1, 2];
        coord_transport.send(msg.clone()).unwrap();
    }
}
