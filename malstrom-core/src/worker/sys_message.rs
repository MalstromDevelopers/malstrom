use indexmap::IndexSet;
use tokio::sync::mpsc;

use crate::types::WorkerId;

/// The standard message is not Send so we use this
/// type to send messages from the coordination runtime
/// to the operator runtime
pub(crate) enum SysMessage<P> {
    Snapshot {
        client: P,
        callback: mpsc::Sender<()>,
    },
    Reconfigure {
        new_set: IndexSet<WorkerId>,
        new_version: u64,
        callback: mpsc::Sender<()>,
    },
    Suspend {
        callback: mpsc::Sender<()>,
    },
}
