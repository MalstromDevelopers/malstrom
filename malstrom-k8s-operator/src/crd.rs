use std::{collections::BTreeMap, net::IpAddr};

use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Kubernetes CRD representing a Jetstream job.
#[derive(CustomResource, Serialize, Deserialize, Debug, PartialEq, Clone, JsonSchema)]
#[kube(
    group = "malstrom.io",
    version = "v1",
    kind = "MalstromJob",
    plural = "malstromjobs",
    derive = "PartialEq",
    status = "JobStatus",
    namespaced
)]
pub struct MalstromJobSpec {
    /// Replica count of deployment. Note: Signed int used to be conformant with Kubernetes API
    pub replicas: i32,
    /// For simple prototypes the operator allows uploading a job binary directly
    #[serde(default)]
    pub binary_name: Option<String>,
    /// Pod template for statefulset
    #[serde(default)]
    pub pod_template: k8s_openapi::api::core::v1::PodTemplateSpec,
    #[serde(default)]
    pub networking: NetworkingSpec,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, JsonSchema)]
pub enum JobStatus {
    Running,
    Finished,
    Failing,
    Failed,
    ScalingUp,
    ScalingDown,
    Suspending,
    Suspended,
}

#[derive(Serialize, Deserialize, Default, Debug, PartialEq, Clone, JsonSchema)]
pub struct NetworkingSpec {
    /// Inbound buffer capacity (message count) for inter-worker communication
    #[serde(default)]
    pub inbound_buffer_capacity: Option<u64>,
    /// Outbound buffer capacity (message count) for inter-worker communication
    #[serde(default)]
    pub outbound_buffer_capacity: Option<u64>,
    /// Socket bind address for inter-worker communication
    #[serde(default)]
    pub bind_addr: Option<IpAddr>,
    /// Port for inter-worker communication
    #[serde(default = "default_port")]
    pub port: u16,
    /// Inter-worker communication initial connection timeout in seconds
    #[serde(default)]
    pub initial_conn_timeout_sec: Option<u64>,
    /// Name of the kubernetes service used to discover worker pods
    pub service_name: String,
}

const fn default_port() -> u16 {
    29091
}

macro_rules! populate_map {
    ($map:ident, $(($key:expr, $field:expr)),* $(,)?) => {
        $(
            if let Some(value) = $field.map(|x| x.to_string()) {
                $map.insert($key.to_string(), value);
            }
        )*
    };
}
impl NetworkingSpec {
    pub(crate) fn to_map(&self) -> BTreeMap<String, String> {
        let mut map: BTreeMap<String, String> = BTreeMap::new();
        populate_map!(
            map,
            ("MALSTROM_K8S_IN_BUF_CAP", self.inbound_buffer_capacity),
            ("MALSTROM_K8S_OUT_BUF_CAP", self.outbound_buffer_capacity),
            ("MALSTROM_K8S_BIND_ADDR", self.bind_addr),
            ("MALSTROM_K8S_PORT", Some(self.port)),
            (
                "MALSTROM_K8S_INIT_CONN_TIMEOUT",
                self.initial_conn_timeout_sec
            )
        );

        map
    }
}
