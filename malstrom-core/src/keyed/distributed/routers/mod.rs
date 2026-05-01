use crate::{
    channels::spsc,
    keyed::{
        WorkerPartitioner,
        distributed::{
            Collect, Interrogate, targeted_message::TargetedData, versioned_message::VersionedData,
            wire_message::WireAcquire,
        },
    },
    stream::BuildContext,
    types::{Key, Kvt, ReconfigComplete, RescaleMessage, distributable::Distributable},
};

mod normal;
use normal::NormalRouter;

mod interrogate;
use interrogate::InterrogateRouter;

mod collect;
use collect::CollectRouter;

mod upgrading;
use tokio::runtime::LocalRuntime;
use upgrading::UpgradingRouter;
/// Message types which go into a router to either be routed or configure the router
pub(super) enum RouterInput<M: Kvt> {
    DataMessage(VersionedData<M>),
    Rescale(RescaleMessage),
    Complete(ReconfigComplete),
}

/// Message types which can be emitted by a router
pub(super) enum RouterOutput<M: Kvt> {
    DataMessage(TargetedData<M>),
    Rescale(RescaleMessage),
    Complete(ReconfigComplete),
    Collect(Collect<M::Key>),
    Acquire(WireAcquire<M::Key>),
    Interrogate(Interrogate<M::Key>),
}

pub(super) struct MessageRouter<M: Kvt> {
    /// Sender to send messages into the router
    pub input: spsc::Sender<RouterInput<M>>,
    /// Receiver to retrieve messages from the router
    pub output: spsc::Receiver<RouterOutput<M>>,
    /// inner router, thes needs to be its own task because [RouterKind::apply] must take self by
    /// ownership which for one makes the method not cancel safe and also makes it not work if
    /// we only have &mut self, like in [Logic::apply]
    routing_task: tokio::task::JoinHandle<()>,
}

impl<M> MessageRouter<M>
where
    M: Kvt,
    M::Key: Key + Distributable,
{
    pub(super) fn spawn_new(ctx: &BuildContext, partition_func: WorkerPartitioner<M::Key>) -> Self {
        let (input_tx, input_rx) = spsc::unbounded();
        let (output_tx, output_rx) = spsc::unbounded();

        let normal_router = NormalRouter::new(
            ctx.config_version,
            ctx.worker_id,
            partition_func,
            ctx.get_worker_ids().to_owned(),
        );
        let router = RouterKind::Normal(normal_router);
        let routing_task = ctx.operator_rt.spawn_local(async move {
            loop {
                router = router.apply(&mut input_rx, &mut output_tx).await
            }
        });

        Self {
            input: input_tx,
            output: output_rx,
            routing_task,
        }
    }
}

/// Types of routers depending on cluster reconfiguration process stage
enum RouterKind<M: Kvt> {
    Normal(NormalRouter<M>),
    Interrogating(InterrogateRouter<M>),
    Collecting(CollectRouter<M>),
    Upgrading(UpgradingRouter<M>),
}

impl<M> RouterKind<M>
where
    M: Kvt,
    M::Key: Key + Distributable,
{
    async fn apply(
        self,
        input: &mut spsc::Receiver<RouterInput<M>>,
        output: &mut spsc::Sender<RouterOutput<M>>,
    ) -> Self {
        match self {
            RouterKind::Normal(router) => router.apply(input, output).await,
            RouterKind::Interrogating(router) => router.apply(input, output).await,
            RouterKind::Collecting(router) => router.apply(input, output).await,
            RouterKind::Upgrading(router) => router.apply(input, output).await,
        }
    }
}
