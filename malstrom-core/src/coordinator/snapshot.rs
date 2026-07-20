use std::time::Duration;

use tracing::{error, info};

use crate::coordinator::api::{ApiRequest, ApiRequestError, ApiRequestOperation};

/// Thread for performing automatic interval snapshots
pub(super) async fn auto_snapshot(snapshot_interval: Duration, req_tx: flume::Sender<ApiRequest>) {
    loop {
        tokio::time::sleep(snapshot_interval).await;
        match ApiRequest::send(ApiRequestOperation::Snapshot, req_tx.clone()).await {
            Ok(_) => info!("Completed automatic snapshot"),
            Err(ApiRequestError::NotRunning | ApiRequestError::Stopped) => {
                error!(
                    "Snapshot failed, coordinator not running. No further snapshots will be attempted"
                );
                return;
            }
        }
    }
}
