pub(crate) mod distributor;

mod acquire;
pub use acquire::Acquire;

mod interrogate;
pub use interrogate::Interrogate;

mod collect;
pub use collect::Collect;

mod remote_receiver;
mod remote_sender;

mod routers;
mod targeted_message;
mod versioned_message;
mod wire_message;

/// Version of the current cluster configuration.
/// TODO: move to global crate scope
type ConfigVersion = u64;
