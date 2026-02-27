//! # Coordinator
//!
//! The [Coordinator] is the brain of any Malstrom job and controls the job lifecycle.
//! It is responsible for triggering job start, snapshots and reconfigurations.
mod api;
#[allow(clippy::module_inception)] // I can't come up with a better name
mod coordinator;
pub use api::CoordinatorApi;
pub use coordinator::{Coordinator, CoordinatorExecutionError};
mod cluster;
pub(crate) mod messages;
mod snapshot;
mod watchmap;

/// This way we do not need separate IDs for worker and coordinator
const COORDINATOR_ID: crate::types::WorkerId = crate::types::WorkerId::MAX;
