use std::{ops::Deref, str::FromStr, time::Duration};

use backon::ConstantBuilder;
use malstrom::types::WorkerId;
use once_cell::sync::Lazy;
use thiserror::Error;

use envconfig::Envconfig;

#[derive(Envconfig)]
pub(crate) struct CommonConfig {
    /// True if this pod is the coordinator
    #[envconfig(from = "MALSTROM_K8S_IS_COORDINATOR", default = "false")]
    pub is_coordinator: bool,

    /// Name of the service exposing the worker pods
    #[envconfig(from = "MALSTROM_K8S_WORKER_SVC_NAME")]
    pub worker_svc_name: String,

    /// Name of the service exposing the coordinator pod
    #[envconfig(from = "MALSTROM_K8S_COORDINATOR_SVC_NAME")]
    pub coordinator_svc_name: String,

    /// startup scale of the cluster, can change later
    /// due to dynamic rescaling
    #[envconfig(from = "MALSTROM_K8S_SCALE")]
    pub initial_scale: u64,
    /// Namespace where this job is deployd
    #[envconfig(from = "MALSTROM_K8S_NS")]
    pub namespace: String,

    /// Name of the worker STS
    #[envconfig(from = "MALSTROM_K8S_WORKER_STS_NAME")]
    pub worker_sts_name: String,

    /// Id of this worker, determined from sts pod-ordinal
    /// This will be unset on the coordinator
    #[envconfig(from = "MALSTROM_K8S_WORKER_HOSTNAME")]
    pub hostname: Option<String>,

    #[envconfig(nested)]
    pub network: NetworkConfig,
}

impl CommonConfig {
    pub(crate) fn get_worker_id(&self) -> WorkerId {
        if self.is_coordinator {
            return WorkerId::MAX;
        }
        let (_, ordinal) = self
            .hostname
            .as_ref()
            .expect("Expected MALSTROM_K8S_WORKER_HOSTNAME to be set")
            .rsplit_once("-")
            .expect("Hostname should follow scheme <sts-name>-<ordinal>");
        WorkerId::from_str(ordinal).expect("Pod ordinal should be a number")
    }
}

#[derive(Envconfig)]
pub(crate) struct NetworkConfig {
    /// max messages in an inbound or outbound buffer
    #[envconfig(from = "MALSTROM_K8S_BUF_CAP", default = "1024")]
    pub buffer_capacity: usize,
    /// Port where local communication server will bind
    #[envconfig(from = "MALSTROM_K8S_PORT", default = "29091")]
    pub port: u16,
    /// Timeout for initial connection attempt in seconds
    #[envconfig(from = "MALSTROM_K8S_INIT_CONN_TIMEOUT", default = "120")]
    pub initial_conn_timeout_sec: u64,
    /// Time to wait on an individual message send before an error is raised in case
    /// of a full queue
    #[envconfig(from = "MALSTROM_K8S_ENQUEUE_TIMEOUT", default = "30")]
    pub enqeueue_timeout_sec: u64,
}
impl NetworkConfig {
    #[inline]
    pub fn enqeueue_timeout(&self) -> Duration {
        Duration::from_secs(self.enqeueue_timeout_sec)
    }

    pub(crate) fn initial_conn_retry(&self) -> ConstantBuilder {
        let pause = Duration::from_secs(5);
        let max_times = self.initial_conn_timeout_sec / pause.as_secs();
        ConstantBuilder::default()
            .with_delay(pause)
            .with_max_times(max_times.try_into().unwrap())
    }
}
/// Just a wrapper around u64 to allow us to do custom FromStr
pub(crate) struct PodOrdinal(WorkerId);
impl FromStr for PodOrdinal {
    type Err = ConfigError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (_, ordinal) = s
            .rsplit_once("-")
            .ok_or(ConfigError::GetWorkerId(s.to_string()))?;
        let as_num =
            WorkerId::from_str(ordinal).map_err(|_| ConfigError::GetWorkerId(s.to_string()))?;
        Ok(Self(as_num))
    }
}
impl Deref for PodOrdinal {
    type Target = WorkerId;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug, Error)]
pub(crate) enum ConfigError {
    #[error("Cannot find worker id in hostname `{0}`")]
    GetWorkerId(String),
}

// this does not change over the pod lifetime so we keep it in a cell
#[cfg(test)]
pub(crate) static CONFIG: Lazy<CommonConfig> = Lazy::new(load_test_config);
#[cfg(not(test))]
pub(crate) static CONFIG: Lazy<CommonConfig> = Lazy::new(load_config);

#[cfg(not(test))]
fn load_config() -> CommonConfig {
    CommonConfig::init_from_env().expect("Necessary env vars should be set")
}

#[cfg(test)]
fn load_test_config() -> CommonConfig {
    use std::collections::HashMap;

    CommonConfig {
        is_coordinator: false,
        worker_svc_name: "worker-svc.default.svc.cluster.locar".into(),
        coordinator_svc_name: "coordinator-svc.default.svc.cluster.locar".into(),
        initial_scale: 1,
        namespace: "default".into(),
        worker_sts_name: "worker".into(),
        hostname: Some("worker-0".into()),
        network: NetworkConfig::init_from_hashmap(&HashMap::new()).unwrap(),
    }
}
