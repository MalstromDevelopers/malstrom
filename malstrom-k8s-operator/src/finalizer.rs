use crate::crd::MalstromJob;
use crate::reconciliation::ReconciliationError;
use kube::api::{Patch, PatchParams};
use kube::{Api, Client};
use serde_json::{json, Value};

/// Add finalizer to CRD
pub async fn add(
    client: Client,
    name: &str,
    namespace: &str,
) -> Result<MalstromJob, ReconciliationError> {
    let api: Api<MalstromJob> = Api::namespaced(client, namespace);
    let finalizer: Value = json!({
        "metadata": {
            "finalizers": ["jetstream/finalizer"]
        }
    });

    let patch: Patch<&Value> = Patch::Merge(&finalizer);
    Ok(api.patch(name, &PatchParams::default(), &patch).await?)
}

/// Remove finalizers from CRD
pub async fn delete(
    client: Client,
    name: &str,
    namespace: &str,
) -> Result<MalstromJob, ReconciliationError> {
    let api: Api<MalstromJob> = Api::namespaced(client, namespace);
    let finalizer: Value = json!({
        "metadata": {
            "finalizers": null
        }
    });

    let patch: Patch<&Value> = Patch::Merge(&finalizer);
    Ok(api.patch(name, &PatchParams::default(), &patch).await?)
}
