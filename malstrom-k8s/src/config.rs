use std::{
    net::{IpAddr, SocketAddr},
    ops::Deref,
    str::FromStr,
    time::Duration,
};

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
    pub fn get_worker_id(&self) -> WorkerId {
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
pub struct NetworkConfig {
    /// max messages in an inbound or outbound buffer
    #[envconfig(from = "MALSTROM_K8S_BUF_CAP", default = "1024")]
    pub buffer_capacity: usize,
    /// Address where server will bind, by defaul 0.0.0.0
    #[envconfig(from = "MALSTROM_K8S_BIND_ADDR", default = "0.0.0.0")]
    pub bind_addr: IpAddr,
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
    pub fn initial_conn_timeout(&self) -> Duration {
        Duration::from_secs(self.initial_conn_timeout_sec)
    }
    #[inline]
    pub fn enqeueue_timeout(&self) -> Duration {
        Duration::from_secs(self.enqeueue_timeout_sec)
    }

    pub(crate) fn initial_conn_retry(&self) -> ConstantBuilder {
        let pause = Duration::from_secs(5);
        let max_times = self.initial_conn_timeout().as_secs() / pause.as_secs();
        ConstantBuilder::default()
            .with_delay(pause)
            .with_max_times(max_times.try_into().unwrap())
    }

    pub(crate) fn get_bind_addr(&self) -> SocketAddr {
        SocketAddr::new(self.bind_addr, self.port)
    }
}
/// Just a wrapper around u64 to allow us to do custom FromStr
pub struct PodOrdinal(WorkerId);
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
pub enum ConfigError {
    #[error("Cannot find worker id in hostname `{0}`")]
    GetWorkerId(String),
}

// this does not change over the pod lifetime so we keep it in a cell

pub(crate) static CONFIG: Lazy<CommonConfig> = Lazy::new(load_config);

fn load_config() -> CommonConfig {
    CommonConfig::init_from_env().expect("Necessary env vars should be set")
}
