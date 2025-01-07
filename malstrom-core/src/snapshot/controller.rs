use indexmap::IndexMap;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::channels::operator_io::Output;
use crate::runtime::communication::broadcast;
use crate::runtime::CommunicationClient;
use crate::snapshot::Barrier;
use crate::stream::{BuildContext, Logic, OperatorContext};
use crate::types::*;

use crate::{channels::operator_io::Input, stream::OperatorBuilder};

use super::triggers::SnapshotTrigger;
use super::{PersistenceBackend, SnapshotVersion};

#[derive(Serialize, Deserialize, Clone)]
pub enum ComsMessage {
    StartSnapshot(SnapshotVersion),
    CommitSnapshot(SnapshotVersion),
}

#[derive(Default)]
struct ControllerState {
    commited_snapshots: IndexMap<WorkerId, SnapshotVersion>,
}

fn build_controller_logic<P: PersistenceBackend>(
    build_context: &mut BuildContext,
    backend: P,
    trigger: impl SnapshotTrigger,
) -> Box<dyn Logic<NoKey, NoData, NoTime, NoKey, NoData, NoTime>> {
    if build_context.worker_id == 0 {
        Box::new(build_leader_controller_logic(
            build_context,
            backend,
            trigger,
        ))
    } else {
        Box::new(build_follower_controller_logic(backend, build_context))
    }
}
fn pass_messages<K: Clone, V: Clone, T: MaybeTime>(
    input: &mut Input<K, V, T>,
    output: &mut Output<K, V, T>,
) {
    match input.recv() {
        Some(Message::AbsBarrier(_)) => {
            unimplemented!("Barriers must not cross persistance regions!")
        }
        Some(x) => output.send(x),
        None => (),
    };
}

fn build_leader_controller_logic<P: PersistenceBackend>(
    build_context: &mut BuildContext,
    backend: P,
    mut trigger: impl SnapshotTrigger,
) -> impl Logic<NoKey, NoData, NoTime, NoKey, NoData, NoTime> {
    let mut last_committed: Option<SnapshotVersion> = build_context.load_state();

    // this is ephemeral state which lets us know if a worker has already committed
    // the currently executing snapshot
    // it also serves double duty as a flag showing whether a snapshot is in progress:
    // A snapshot is in progress if this is Some
    let mut in_progress_snapshot: Option<(IndexMap<WorkerId, bool>, SnapshotVersion, Barrier)> =
        None;

    let worker_ids = build_context
        .get_worker_ids()
        .iter()
        .map(|x| *x)
        .collect_vec();
    let this_worker = build_context.worker_id;
    let clients: IndexMap<WorkerId, CommunicationClient<ComsMessage>> = worker_ids
        .iter()
        .filter(|i| **i != this_worker)
        .cloned()
        .map(|i| (i, build_context.create_communication_client(i)))
        .collect();

    move |input: &mut Input<NoKey, NoData, NoTime>,
          output: &mut Output<NoKey, NoData, NoTime>,
          ctx: &mut OperatorContext| {
        pass_messages(input, output);

        if in_progress_snapshot.is_none() && trigger.should_trigger() {
            info!("Starting global snapshot");
            // start a new global snapshot
            let snapshot_version = last_committed.unwrap_or(0);
            let backend_client = backend.for_version(ctx.worker_id, &snapshot_version);
            let barrier = Barrier::new(Box::new(backend_client));

            in_progress_snapshot = Some(
                // add workerid 0 for this worker
                (
                    worker_ids.iter().cloned().map(|x| (x, false)).collect(),
                    snapshot_version,
                    barrier.clone(),
                ),
            );

            // instruct other workers to start snapshotting
            broadcast(
                clients.values(),
                ComsMessage::StartSnapshot(snapshot_version),
            );
            output.send(Message::AbsBarrier(barrier.clone()));
        }

        if let Some((ref mut commits, snapshot_version, ref mut barrier)) = in_progress_snapshot {
            if barrier.strong_count() == 1 {
                // if we are holding onto the only remaining barrier it means
                // the local snapshot has completed
                commits.insert(ctx.worker_id, true);
            }

            for (wid, client) in clients.iter() {
                for msg in client.recv_all() {
                    match msg {
                        ComsMessage::StartSnapshot(_i) => {
                            unreachable!(
                                "Lead node received instruction which can only be given by lead node"
                            )
                        }
                        ComsMessage::CommitSnapshot(_i) => {
                            commits.insert(*wid, true);
                        }
                    }
                }
            }

            if commits.values().all(|x| *x) {
                info!("Global snapshot finished");
                // all workers have responded with a commit
                barrier.persist(&snapshot_version, &ctx.operator_id);
                last_committed = Some(snapshot_version);
                in_progress_snapshot = None;
                backend.commit_version(&snapshot_version);
            }
        }
    }
}

fn build_follower_controller_logic<P: PersistenceBackend>(
    backend: P,
    build_context: &mut BuildContext,
) -> impl Logic<NoKey, NoData, NoTime, NoKey, NoData, NoTime> {
    let mut in_progress: Option<Barrier> = None;

    let leader = build_context.create_communication_client(0);

    move |input: &mut Input<NoKey, NoData, NoTime>,
          output: &mut Output<NoKey, NoData, NoTime>,
          ctx: &mut OperatorContext| {
        pass_messages(input, output);

        in_progress = match in_progress.take() {
            Some(b) => {
                if b.strong_count() == 1 {
                    let version = b.get_version();
                    leader.send(ComsMessage::CommitSnapshot(version));
                    None
                } else {
                    Some(b)
                }
            }
            None => None,
        };

        while let Some(msg) = leader.recv() {
            match msg {
                ComsMessage::StartSnapshot(i) => {
                    let client = backend.for_version(ctx.worker_id, &i);
                    let barrier = Barrier::new(Box::new(client));
                    in_progress = Some(barrier.clone());
                    output.send(Message::AbsBarrier(barrier))
                }
                ComsMessage::CommitSnapshot(_) => {
                    unreachable!("Follower node can not receive a snapshot commit")
                }
            }
        }
    }
}

pub fn make_snapshot_controller<P: PersistenceBackend>(
    backend: P,
    trigger: impl SnapshotTrigger,
) -> OperatorBuilder<NoKey, NoData, NoTime, NoKey, NoData, NoTime> {
    // we can just use a static name here since we know there will only ever
    // be one snapshot controller
    let op = OperatorBuilder::built_by("malstrom::snapshot-controller", |ctx| {
        build_controller_logic(ctx, backend, trigger)
    });
    // indicate we will never emit data records
    // op.get_output_mut().send(crate::Message::Epoch(NoTime::MAX));
    op
}

#[cfg(test)]
mod tests {}
