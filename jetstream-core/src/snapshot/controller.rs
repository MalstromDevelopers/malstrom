use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

use crate::channels::selective_broadcast::Sender;
use crate::snapshot::Barrier;
use crate::stream::operator::{BuildContext, Logic, OperatorContext};
use crate::time::{MaybeTime, NoTime};
use crate::{Data, MaybeKey, Message, NoData, NoKey, WorkerId};

use crate::{channels::selective_broadcast::Receiver, stream::operator::OperatorBuilder};

use super::SnapshotVersion;
use super::{Load, PersistenceBackend};

#[derive(Serialize, Deserialize)]
pub enum ComsMessage {
    StartSnapshot(SnapshotVersion),
    LoadSnapshot(SnapshotVersion),
    CommitSnapshot(WorkerId, SnapshotVersion),
}

type BackchannelTx<P> = Sender<NoData, NoKey, NoTime, P>;
type BackchannelRx<P> = Receiver<NoData, NoKey, NoTime, P>;

// #[derive(Clone)]
// pub struct RegionHandle<P> {
//     sender: BackchannelTx<P>
// }

#[derive(Default)]
struct ControllerState {
    commited_snapshots: IndexMap<WorkerId, SnapshotVersion>,
}

fn build_controller_logic<K: MaybeKey, V: Data, T: MaybeTime, P: PersistenceBackend>(
    build_context: &BuildContext<P>,
    timer: impl FnMut() -> bool + 'static,
) -> Box<dyn Logic<K, V, T, K, V, T, P>> {
    // TODO: Commit snapshot to backend
    if build_context.worker_id == 0 {
        Box::new(build_leader_controller_logic(build_context, timer))
    } else {
        Box::new(build_follower_controller_logic())
    }
}
fn pass_messages<K: Clone, V: Clone, T: Clone, P>(
    input: &mut Receiver<K, V, T, P>,
    output: &mut Sender<K, V, T, P>,
) {
    match input.recv() {
        Some(Message::AbsBarrier(_)) => {
            unimplemented!("Barriers must not cross persistance regions!")
        }
        Some(Message::Load(_)) => {
            unimplemented!("Loads must not cross persistance regions!")
        }
        Some(x) => output.send(x),
        None => (),
    };
}

fn build_leader_controller_logic<K: MaybeKey, V: Data, T: MaybeTime, P: PersistenceBackend>(
    build_context: &BuildContext<P>,
    mut timer: impl FnMut() -> bool + 'static,
) -> impl Logic<K, V, T, K, V, T, P> {
    let mut last_committed: Option<SnapshotVersion> = build_context.load_state();

    // this is ephemeral state which lets us know if a worker has already committed
    // the currently executing snapshot
    // it also serves double duty as a flag showing whether a snapshot is in progress:
    // A snapshot is in progress if this is Some
    let mut in_progress_snapshot: Option<(IndexMap<WorkerId, bool>, SnapshotVersion, Barrier<P>)> =
        None;

    move |input: &mut Receiver<K, V, T, P>,
          output: &mut Sender<K, V, T, P>,
          ctx: &mut OperatorContext| {
        pass_messages(input, output);

        if in_progress_snapshot.is_some() && timer() {
            // start a new global snapshot
            let snapshot_version = last_committed.unwrap_or(0);
            let barrier = Barrier::new(P::new_for_version(ctx.worker_id, &snapshot_version));

            let _ = in_progress_snapshot.insert(
                // add workerid 0 for this worker
                (
                    ctx.communication
                        .get_peers()
                        .into_iter()
                        .chain(std::iter::once(&0usize))
                        .map(|x| (x.to_owned(), false))
                        .collect(),
                    snapshot_version,
                    barrier.clone(),
                ),
            );

            println!("Starting new snapshot at epoch {snapshot_version}");

            // instruct other workers to start snapshotting
            ctx.communication
                .broadcast(ComsMessage::StartSnapshot(snapshot_version))
                .expect("Network communication error");
            output.send(Message::AbsBarrier(barrier));
        }

        if let Some((ref mut commits, snapshot_version, ref mut backend)) = in_progress_snapshot {
            if backend.strong_count() == 1 {
                // if we are holding onto the only remaining barrier it means
                // the local snapshot has completed
                commits.insert(ctx.worker_id, true);
            }

            for msg in ctx.communication.recv_all() {
                match msg {
                    ComsMessage::StartSnapshot(_i) => {
                        unreachable!(
                            "Lead node received instruction which can only be given by lead node"
                        )
                    }
                    ComsMessage::LoadSnapshot(_i) => {
                        unreachable!(
                            "Lead node received instruction which can only be given by lead node"
                        )
                    }
                    ComsMessage::CommitSnapshot(wid, _i) => {
                        commits.insert(wid, true);
                    }
                }
            }

            if commits.values().all(|x| *x) {
                // all workers have responded with a commit
                backend.persist(&snapshot_version, ctx.operator_id);
                last_committed = Some(snapshot_version);
                in_progress_snapshot = None;
            }
        }
    }
}

fn build_follower_controller_logic<K: MaybeKey, V: Data, T: MaybeTime, P: PersistenceBackend>(
) -> impl Logic<K, V, T, K, V, T, P> {
    let mut in_progress: Option<Barrier<P>> = None;

    move |input: &mut Receiver<K, V, T, P>,
          output: &mut Sender<K, V, T, P>,
          ctx: &mut OperatorContext| {
        pass_messages(input, output);

        in_progress = match in_progress.take() {
            Some(b) => {
                if b.strong_count() == 1 {
                    let version = b.get_version();
                    println!("Completed snapshot at {version}");
                    ctx.communication
                        .send(&0, ComsMessage::CommitSnapshot(ctx.worker_id, version))
                        .unwrap();
                    None
                } else {
                    Some(b)
                }
            }
            None => None,
        };

        for msg in ctx.communication.recv_all() {
            match msg {
                ComsMessage::StartSnapshot(i) => {
                    let barrier = Barrier::new(P::new_for_version(ctx.worker_id, &i));
                    in_progress.insert(barrier.clone());
                    output.send(Message::AbsBarrier(barrier))
                }
                ComsMessage::LoadSnapshot(i) => output.send(Message::Load(Load::new(
                    P::new_for_version(ctx.worker_id, &i),
                ))),
                ComsMessage::CommitSnapshot(_wid, _version) => {
                    unreachable!("Follower node can not receive a snapshot commit")
                }
            }
        }
    }
}

pub fn make_snapshot_controller<K: MaybeKey, V: Data, T: MaybeTime, P: PersistenceBackend>(
    timer: impl FnMut() -> bool + 'static,
) -> OperatorBuilder<K, V, T, K, V, T, P> {
    OperatorBuilder::built_by(|ctx| build_controller_logic(ctx, timer))
}

// pub fn end_snapshot_region<K: Key, V: Data, T: Timestamp, P: PersistenceBackend>(
//     stream: JetStreamBuilder<K, V, T, P>,
//     region_handle: RegionHandle<P>,
// ) -> JetStreamBuilder<K, V, T, P> {
//     let op = OperatorBuilder::direct(move |input: &mut Receiver<K, V, T, P>, output, _ctx| {
//         match input.recv() {
//             Some(Message::AbsBarrier(b)) => region_handle.sender.send(b).unwrap(),
//             Some(x) => output.send(x),
//             None => (),
//         };
//     });
//     stream.then(op)
// }
