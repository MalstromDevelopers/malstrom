use std::rc::Rc;

use indexmap::IndexMap;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::channels::selective_broadcast::Sender;
use crate::runtime::communication::broadcast;
use crate::runtime::CommunicationClient;
use crate::snapshot::Barrier;
use crate::stream::operator::{AppendableOperator, BuildContext, Logic, OperatorContext};
use crate::time::{NoTime, Timestamp};
use crate::{Data, MaybeKey, Message, NoData, NoKey, WorkerId};

use crate::{channels::selective_broadcast::Receiver, stream::operator::OperatorBuilder};

use super::{PersistenceBackend, SnapshotVersion};

#[derive(Serialize, Deserialize, Clone)]
pub enum ComsMessage {
    StartSnapshot(SnapshotVersion),
    LoadSnapshot(SnapshotVersion),
    CommitSnapshot(SnapshotVersion),
}

type BackchannelTx = Sender<NoData, NoKey, NoTime>;
type BackchannelRx = Receiver<NoData, NoKey, NoTime>;

// #[derive(Clone)]
// pub struct RegionHandle {
//     sender: BackchannelTx
// }

#[derive(Default)]
struct ControllerState {
    commited_snapshots: IndexMap<WorkerId, SnapshotVersion>,
}

fn build_controller_logic<K: MaybeKey, V: Data, T>(
    build_context: &mut BuildContext,
    backend_builder: Rc<dyn PersistenceBackend>,
    timer: impl FnMut() -> bool + 'static,
) -> Box<dyn Logic<K, V, T, K, V, T>> {
    // TODO: Commit snapshot to backend
    if build_context.worker_id == 0 {
        Box::new(build_leader_controller_logic(
            build_context,
            backend_builder,
            timer,
        ))
    } else {
        Box::new(build_follower_controller_logic(
            backend_builder,
            build_context,
        ))
    }
}
fn pass_messages<K: Clone, V: Clone, T: Timestamp>(
    input: &mut Receiver<K, V, T>,
    output: &mut Sender<K, V, T>,
) {
    match input.recv() {
        Some(Message::AbsBarrier(_)) => {
            unimplemented!("Barriers must not cross persistance regions!")
        }
        // Some(Message::Load(_)) => {
        //     unimplemented!("Loads must not cross persistance regions!")
        // }
        Some(x) => output.send(x),
        None => (),
    };
}

fn build_leader_controller_logic<K: MaybeKey, V: Data, T: Timestamp>(
    build_context: &mut BuildContext,
    backend_builder: Rc<dyn PersistenceBackend>,
    mut timer: impl FnMut() -> bool + 'static,
) -> impl Logic<K, V, T, K, V, T> {
    let mut last_committed: Option<SnapshotVersion> = build_context.load_state();

    // this is ephemeral state which lets us know if a worker has already committed
    // the currently executing snapshot
    // it also serves double duty as a flag showing whether a snapshot is in progress:
    // A snapshot is in progress if this is Some
    let mut in_progress_snapshot: Option<(IndexMap<WorkerId, bool>, SnapshotVersion, Barrier)> =
        None;

    let worker_ids = build_context.get_worker_ids().collect_vec();
    let clients: IndexMap<WorkerId, CommunicationClient<ComsMessage>> = worker_ids
        .iter()
        .cloned()
        .filter_map(|i| {
            if i != build_context.worker_id {
                Some((
                    i,
                    build_context.create_communication_client(i, build_context.operator_id),
                ))
            } else {
                None
            }
        })
        .collect();

    move |input: &mut Receiver<K, V, T>, output: &mut Sender<K, V, T>, ctx: &mut OperatorContext| {
        pass_messages(input, output);

        if in_progress_snapshot.is_none() && timer() {
            // start a new global snapshot
            let snapshot_version = last_committed.unwrap_or(0);
            let backend = backend_builder.for_version(ctx.worker_id, &snapshot_version);
            let barrier = Barrier::new(backend);

            let _ = in_progress_snapshot.insert(
                // add workerid 0 for this worker
                (
                    worker_ids.iter().cloned().map(|x| (x, false)).collect(),
                    snapshot_version,
                    barrier.clone(),
                ),
            );

            println!("Starting new snapshot at epoch {snapshot_version}");

            // instruct other workers to start snapshotting
            broadcast(
                clients.values(),
                ComsMessage::StartSnapshot(snapshot_version),
            );
            output.send(Message::AbsBarrier(barrier));
        }

        if let Some((ref mut commits, snapshot_version, ref mut backend)) = in_progress_snapshot {
            if backend.strong_count() == 1 {
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
                        ComsMessage::LoadSnapshot(_i) => {
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
                // all workers have responded with a commit
                backend.persist(&snapshot_version, &ctx.operator_id);
                last_committed = Some(snapshot_version);
                in_progress_snapshot = None;
            }
        }
    }
}

fn build_follower_controller_logic<K: MaybeKey, V: Data, T: Timestamp>(
    backend_builder: Rc<dyn PersistenceBackend>,
    build_context: &mut BuildContext,
) -> impl Logic<K, V, T, K, V, T> {
    let mut in_progress: Option<Barrier> = None;

    let leader = build_context.create_communication_client(0, build_context.operator_id);

    move |input: &mut Receiver<K, V, T>, output: &mut Sender<K, V, T>, ctx: &mut OperatorContext| {
        pass_messages(input, output);

        in_progress = match in_progress.take() {
            Some(b) => {
                if b.strong_count() == 1 {
                    let version = b.get_version();
                    println!("Completed snapshot at {version}");
                    leader.send(ComsMessage::CommitSnapshot(version));
                    None
                } else {
                    Some(b)
                }
            }
            None => None,
        };

        for msg in leader.recv_all() {
            match msg {
                ComsMessage::StartSnapshot(i) => {
                    let barrier = Barrier::new(backend_builder.for_version(ctx.worker_id, &i));
                    in_progress = Some(barrier.clone());
                    output.send(Message::AbsBarrier(barrier))
                }
                ComsMessage::LoadSnapshot(_) => todo!(),
                // output.send(Message::Load(Load::new(
                //     P::new_for_version(ctx.worker_id, &i),
                // ))),
                ComsMessage::CommitSnapshot(_) => {
                    unreachable!("Follower node can not receive a snapshot commit")
                }
            }
        }
    }
}

pub fn make_snapshot_controller<K: MaybeKey, V: Data, T: Timestamp>(
    backend_builder: Rc<dyn PersistenceBackend>,
    timer: impl FnMut() -> bool + 'static,
) -> OperatorBuilder<K, V, T, K, V,T> {
    let mut op = OperatorBuilder::built_by(|ctx| build_controller_logic(ctx, backend_builder, timer));
    // indicate we will never emit data records
    // op.get_output_mut().send(crate::Message::Epoch(NoTime::MAX));
    op
}

// pub fn end_snapshot_region<K: Key, V: Data, T: Timestamp, P: PersistenceBackend>(
//     stream: JetStreamBuilder<K, V, T>,
//     region_handle: RegionHandle,
// ) -> JetStreamBuilder<K, V, T> {
//     let op = OperatorBuilder::direct(move |input: &mut Receiver<K, V, T>, output, _ctx| {
//         match input.recv() {
//             Some(Message::AbsBarrier(b)) => region_handle.sender.send(b).unwrap(),
//             Some(x) => output.send(x),
//             None => (),
//         };
//     });
//     stream.then(op)
// }

#[cfg(test)]
mod tests {
    use crate::snapshot::NoPersistence;
    use super::*;

    /// check we indicate to the worker that this operator does not produce data
    #[test]
    fn indicate_no_output() {
        let mut op = make_snapshot_controller::<NoKey, NoData>(Rc::new(NoPersistence::default()), || false);
        assert_eq!(*op.get_output_mut().get_frontier().as_ref().unwrap(), NoTime::MAX);
    }
}