/// All errors possible to occur during reconciliation
#[derive(Debug, thiserror::Error)]
pub enum Error {
    // Wrapper for Kubernetes error
    #[error("Kubernetes reported error: {0}")]
    KubeError(kube::Error),
    #[error("Error connecting to job coordinator: {0:?}")]
    CoordinatorConnection(#[from] tonic::transport::Error),
    #[error(transparent)]
    Any(#[from] anyhow::Error)
}

impl From<kube::Error> for Error {
    fn from(e: kube::Error) -> Self {
        Self::KubeError(e)
    }
}