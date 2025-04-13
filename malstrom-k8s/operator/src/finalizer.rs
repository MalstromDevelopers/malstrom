use crds::MalstromJob;
use kube::api::{Patch, PatchParams};
use kube::{Api, Client};
use serde_json::{json, Value};
use thiserror::Error;

/// Add finalizer to CRD
pub async fn add(
    client: Client,
    name: &str,
    namespace: &str,
) -> Result<MalstromJob, AddFinalizerError> {
    let api: Api<MalstromJob> = Api::namespaced(client, namespace);
    let finalizer: Value = json!({
        "metadata": {
            "finalizers": ["malstrom.io/finalizer"]
        }
    });

    let params = PatchParams::apply("malstrom-operator");

    let patch: Patch<&Value> = Patch::Merge(&finalizer);
    Ok(api.patch(name, &params, &patch).await?)
}

#[derive(Debug, Error)]
pub(crate) enum AddFinalizerError {
    #[error("Error from Kubernetes: {0}")]
    Kubernetes(#[from] kube::Error),
}

/// Remove finalizers from CRD
pub async fn delete(
    client: Client,
    name: &str,
    namespace: &str,
) -> Result<MalstromJob, DeleteFinalizerError> {
    let api: Api<MalstromJob> = Api::namespaced(client, namespace);
    let finalizer: Value = json!({
        "metadata": {
            "finalizers": null
        }
    });

    let params = PatchParams::apply("malstrom-operator");

    let patch: Patch<&Value> = Patch::Merge(&finalizer);
    Ok(api.patch(name, &params, &patch).await?)
}

#[derive(Debug, Error)]
pub(crate) enum DeleteFinalizerError {
    #[error("Error from Kubernetes: {0}")]
    Kubernetes(#[from] kube::Error),
}
