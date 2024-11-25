use std::{net::{IpAddr, SocketAddr}, ops::Deref, str::FromStr, time::Duration};

use backon::ConstantBuilder;
use jetstream::types::WorkerId;
use once_cell::sync::Lazy;
use thiserror::Error;

use envconfig::Envconfig;
use tonic::transport::Endpoint;

#[derive(Envconfig)]
pub struct Config {
    /// startup scale of the cluster, can change later
    /// due to dynamic rescaling
    #[envconfig(from = "MALSTROM_K8S_SCALE")]
    pub initial_scale: u64,

    /// Namespace of the cluster statefulset
    #[envconfig(from = "MALSTROM_K8S_STS_NS")]
    pub sts_namespace: String,
    
    /// Name of the service to use for pod discovery, typically
    /// the statefulsets headless service
    #[envconfig(from = "MALSTROM_K8S_SVC_NAME")]
    pub svc_name: String,

    /// Id of this worker, determined from sts pod-ordinal
    #[envconfig(from = "MALSTROM_K8S_HOSTNAME")]
    pub hostname: String,


    #[envconfig(nested)]
    pub network: NetworkConfig
}

impl Config {
    pub fn get_worker_id(&self) -> WorkerId {
        let (_, ordinal) = self.hostname.rsplit_once("-").expect("Hostname should follow scheme <sts-name>-<ordinal>");
        WorkerId::from_str(ordinal).expect("Pod ordinal should be a number")
    }
    pub fn get_sts_name(&self) -> &str {
        let (sts_name, _) = self.hostname.rsplit_once("-").expect("Hostname should follow scheme <sts-name>-<ordinal>");
        sts_name
    }
}

#[derive(Envconfig)]
pub struct NetworkConfig {
    /// max messages in inbound buffer
    #[envconfig(from = "MALSTROM_K8S_IN_BUF_CAP", default = "1024")]
    pub inbound_buf_cap: usize,
    /// max messages in outbound buffer
    #[envconfig(from = "MALSTROM_K8S_OUT_BUF_CAP", default = "1024")]
    pub outbound_buf_cap: usize,
    /// Address where server will bind, by defaul 0.0.0.0
    #[envconfig(from = "MALSTROM_K8S_BIND_ADDR", default = "0.0.0.0")]
    pub bind_addr: IpAddr,
    /// Port where local communication server will bind
    #[envconfig(from = "MALSTROM_K8S_PORT", default = "29091")]
    pub port: u16,
    /// Timeout for initial connection attempt in seconds
    #[envconfig(from = "MALSTROM_K8S_INIT_CONN_TIMEOUT", default = "120")]
    pub initial_conn_timeout_sec: u64
}
impl NetworkConfig {
    pub fn initial_conn_timeout(&self) -> Duration {
        Duration::from_secs(self.initial_conn_timeout_sec)
    }

    pub(crate) fn initial_conn_retry(&self) -> ConstantBuilder {
        let pause = Duration::from_secs(5);
        let max_times = self.initial_conn_timeout().as_secs() / pause.as_secs();
        ConstantBuilder::default().with_delay(pause).with_max_times(max_times.try_into().unwrap())
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
        let (_, ordinal) = s.rsplit_once("-").ok_or(ConfigError::GetWorkerId(s.to_string()))?;
        let as_num = WorkerId::from_str(ordinal).map_err(|_| ConfigError::GetWorkerId(s.to_string()))?;
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
    GetWorkerId(String)
}

// this does not change over the pod lifetime so we keep it in a cell

pub(crate) static CONFIG: Lazy<Config> = Lazy::new(load_config);

fn load_config() -> Config {
    Config::init_from_env().expect("Necessary env vars should be set")
}