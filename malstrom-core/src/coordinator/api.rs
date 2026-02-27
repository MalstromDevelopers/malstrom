use thiserror::Error;

/// API handle for the [Coordinator]. Use this to send commands (like a rescale-command)
/// to the coordinator
pub struct CoordinatorApi {
    req_tx: flume::Sender<ApiRequest>,
}

impl CoordinatorApi {
    pub(super) fn new(req_tx: flume::Sender<ApiRequest>) -> Self {
        Self { req_tx }
    }

    /// Rescale the Malstrom job to a desired parallelism.
    /// This will perform a zero-downtime rescaling and distribute all worker state accordingly.
    /// If the desired scale == current scale, this is a no-op.
    ///
    /// # Arguments
    /// - desired: Desired parallelism
    pub async fn rescale(&self, desired: u64) -> Result<(), ApiRequestError> {
        ApiRequest::send(ApiRequestOperation::Scale(desired), self.req_tx.clone()).await
    }
}

/// New request for what the coordinator should do
#[derive(Debug)]
pub(super) struct ApiRequest {
    /// Oneshot which completes as soon as the coordinator has fullfilled the request
    /// or encountered and error
    pub(super) callback: tokio::sync::oneshot::Sender<Result<(), ApiRequestError>>,
    /// Operation to perform
    pub(super) request: ApiRequestOperation,
}
impl ApiRequest {
    /// Send a new request of the given operation to the coordinator. Future resolves as soon as
    /// the request has been completed or errored
    pub(super) async fn send(
        request: ApiRequestOperation,
        channnel: flume::Sender<ApiRequest>,
    ) -> Result<(), ApiRequestError> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let req = ApiRequest {
            callback: tx,
            request,
        };
        channnel
            .send_async(req)
            .await
            .map_err(|_| ApiRequestError::NotRunning)?;
        rx.await.map_err(|_| ApiRequestError::Stopped)?
    }
}

/// Request the Coordinator to do something
#[derive(Debug, Clone, Copy)]
pub(super) enum ApiRequestOperation {
    /// Request coordinator to perform a global state snapshot
    Snapshot,
    /// Request coordinator to rescale the job to this parallelism
    Scale(u64),
    /// UNIMPLEMENTED: Request Coordinator to suspend the execution
    #[allow(unused)] // TODO
    Suspend,
}

/// Possible errors returned by Coordinator when asked to perform requested Action
#[allow(missing_docs)]
#[derive(Debug, Error)]
pub enum ApiRequestError {
    #[error("Coordinator is not running")]
    NotRunning,
    #[error("Coordinator stopped while performing operation")]
    Stopped,
}
