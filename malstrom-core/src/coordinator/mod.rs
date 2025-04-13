//! # Coordinator
//!
//! The [Coordinator] is the brain of any Malstrom job and controls the job lifecycle.
//! It is responsible for triggering job start, snapshots and reconfigurations.

#[allow(clippy::module_inception)] // I can't come up with a better name
mod coordinator;
pub use coordinator::{
    Coordinator, CoordinatorApi, CoordinatorExecutionError, CoordinatorRequestError,
    CoordinatorStatus,
};
mod communication;
mod state;
pub(crate) mod types;
mod watchmap;
