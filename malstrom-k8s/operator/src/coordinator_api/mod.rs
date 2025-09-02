use std::time::Duration;

use tonic::transport::{Channel, Endpoint};

mod proto {
    tonic::include_proto!("malstrom_k8s.k8s_operator");
}
pub use proto::coordinator_operator_service_client::CoordinatorOperatorServiceClient;
pub use proto::RescaleRequest;

pub async fn get_coord_api_client(
    endpoint: Endpoint,
) -> Result<CoordinatorOperatorServiceClient<Channel>, tonic::transport::Error> {
    let endpoint = endpoint.connect_timeout(Duration::from_secs(20));
    CoordinatorOperatorServiceClient::connect(endpoint).await
}
