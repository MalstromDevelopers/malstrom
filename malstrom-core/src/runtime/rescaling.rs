
use crate::{
    channels::operator_io::{Input, Output},
    runtime::communication::broadcast,
    stream::{BuildContext, Logic, OperatorBuilder, OperatorContext},
    types::{Message, NoData, NoKey, NoTime, RescaleChange, RescaleMessage, WorkerId},
};
use indexmap::{IndexMap, IndexSet};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::oneshot::{Receiver, Sender};
use tracing::info;


pub(super) type RescaleController = OperatorBuilder<NoKey, NoData, NoTime, NoKey, NoData, NoTime>;

pub(super) struct RescaleRequest {
    change: RescaleRequestChange,
    callback: Sender<RescaleResponse>,
}
impl RescaleRequest {
    pub(super) fn new(change: i64) -> (Self, Receiver<RescaleResponse>) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        // PANIC: Will always fit because of abs
        let net_change: u64 = change.abs().try_into().unwrap();
        let this = if change > 0 {
            Self {
                change: RescaleRequestChange::ScaleUp(net_change),
                callback: tx,
            }
        } else {
            Self {
                change: RescaleRequestChange::ScaleDown(net_change),
                callback: tx,
            }
        };
        (this, rx)
    }

    fn finish(self, resp: RescaleResponse) -> () {
        // Ignore error since we do not care if other end was dropped
        let _ = self.callback.send(resp);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) enum RescaleRequestChange {
    ScaleUp(u64),
    ScaleDown(u64),
}

pub(super) type RescaleResponse = Result<(), RescaleError>;

#[derive(Debug, Error)]
pub enum RescaleError {
    #[error("Attempted to downscale to 0 workers or less")]
    DownscaleTooFar,
    #[error("Rescale can not be initiated on follower worker")]
    InitiatedOnFollower,
}

pub fn make_rescale_controller(
    channel: std::sync::mpsc::Receiver<RescaleRequest>,
) -> OperatorBuilder<NoKey, NoData, NoTime, NoKey, NoData, NoTime> {
    // we can just use a static name here since we know there will only ever
    // be one snapshot controller
    let op = OperatorBuilder::built_by("malstrom::scaling-controller", |ctx| {
        build_controller_logic(ctx, channel)
    });
    op
}

fn build_controller_logic(
    build_context: &mut BuildContext,
    request_channel: std::sync::mpsc::Receiver<RescaleRequest>,
) -> Box<dyn Logic<NoKey, NoData, NoTime, NoKey, NoData, NoTime>> {
    if build_context.worker_id == 0 {
        Box::new(build_leader_controller_logic(
            build_context,
            request_channel,
        ))
    } else {
        Box::new(run_rescale(
            build_context,
            request_channel,
        ))
    }
}

#[derive(Serialize, Deserialize, Clone)]
enum ComsMessage {
    ScaleUp(IndexSet<WorkerId>),
    ScaleDown(IndexSet<WorkerId>),
    Complete,
}

struct InProgressRescale {
    remotes: IndexMap<WorkerId, bool>, // true if worker indicated it has finished the rescale
    local: RescaleMessage,             // local rescale message
    request: RescaleRequest,           // triggering request
}

fn build_leader_controller_logic(
    build_context: &mut BuildContext,
    request_channel: std::sync::mpsc::Receiver<RescaleRequest>,
) -> impl Logic<NoKey, NoData, NoTime, NoKey, NoData, NoTime> {
    let mut in_progress_rescale: Option<InProgressRescale> = None;
    let mut clients = build_context.create_all_communication_clients::<ComsMessage>();

    move |input: &mut Input<NoKey, NoData, NoTime>,
          output: &mut Output<NoKey, NoData, NoTime>,
          ctx: &mut OperatorContext| {
        if let Some(x) = input.recv() {
            output.send(x);
        };

        if in_progress_rescale.is_none() {
            if let Some(req) = request_channel.try_recv().ok() {
                info!("Starting rescaling");
                let current_max = clients.keys().max().unwrap_or(&0);
                let (local_msg, remotes) = match &req.change {
                    RescaleRequestChange::ScaleUp(x) => {
                        let add_set: IndexSet<u64> =
                            ((current_max + 1)..(current_max + 1 + x)).collect();
                        for wid in add_set.iter() {
                            let client = ctx.create_communication_client(*wid);
                            clients.insert(*wid, client);
                        }
                        broadcast(clients.values(), ComsMessage::ScaleUp(add_set.clone()));
                        let remotes = add_set.clone().into_iter().map(|x| (x, true)).collect();
                        (RescaleMessage::new_add(add_set), remotes)
                    }
                    RescaleRequestChange::ScaleDown(x) => {
                        if x >= current_max {
                            // don't care about dropped channel
                            let _ = req.finish(RescaleResponse::Err(RescaleError::DownscaleTooFar));
                            return;
                        }
                        let remove_set: IndexSet<u64> =
                            ((current_max + 1)..(current_max + x)).collect();
                        broadcast(clients.values(), ComsMessage::ScaleDown(remove_set.clone()));
                        let remotes = remove_set.clone().into_iter().map(|x| (x, false)).collect();

                        (RescaleMessage::new_remove(remove_set), remotes)
                    }
                };
                output.send(Message::Rescale(local_msg.clone()));
                in_progress_rescale = Some(InProgressRescale {
                    remotes,
                    local: local_msg,
                    request: req,
                });
            }
        }

        in_progress_rescale = if let Some(mut rescale) = in_progress_rescale.take() {
            if rescale.local.strong_count() == 1 && rescale.remotes.values().all(|x| *x) {
                match rescale.local.get_change() {
                    RescaleChange::ScaleRemoveWorker(index_set) => {
                        for wid in index_set.iter() {
                            clients.shift_remove(wid);
                        }
                    }
                    RescaleChange::ScaleAddWorker(_) => (),
                };
                rescale.request.finish(RescaleResponse::Ok(()));
                info!("Rescaling finished");
                None
            } else {
                for (wid, client) in clients.iter() {
                    match client.recv() {
                        Some(ComsMessage::Complete) => {
                            rescale.remotes.insert(*wid, true);
                        }
                        None => (),
                        _ => unreachable!("Leader can only receive completion"),
                    }
                }
                Some(rescale)
            }
        } else {
            None
        };
    }
}

enum RescaleOperation {
    ScaleAdd(IndexSet<u64>),
    ScaleRemove(IndexSet<u64>),
}

fn run_rescale(
    output: &mut Output<NoKey, NoData, NoTime>,
    trigger: RescaleOperation,
    schedule_fn: &mut impl FnMut() -> bool
) -> () {
    let in_progress_rescale: RescaleMessage = match trigger {
        RescaleOperation::ScaleAdd(index_set) => RescaleMessage::new_add(index_set),
        RescaleOperation::ScaleRemove(index_set) => RescaleMessage::new_remove(index_set),
    };
    output.send(Message::Rescale(in_progress_rescale.clone()));
    while in_progress_rescale.strong_count() > 1 {
        schedule_fn();
    }
    
}
