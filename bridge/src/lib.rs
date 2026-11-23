#![feature(custom_inner_attributes, register_tool)]
#![register_tool(jvm_codegen)]
#![jvm_codegen::export]

mod transport;

use anyhow::{Context, anyhow, bail};
use prost::Message;
use std::{
    collections::HashMap,
    sync::{LazyLock, Mutex},
};
use temporalio_client::{Connection, ConnectionOptions, callback_based::CallbackBasedGrpcService};
use temporalio_common::{
    protos::{
        coresdk::{ActivityTaskCompletion, workflow_completion::WorkflowActivationCompletion},
        temporal::api::{
            deployment::v1::WorkerDeploymentOptions as ProtoDeploymentOptions,
            enums::v1::{VersioningBehavior, WorkerVersioningMode},
            failure::v1::Failure,
        },
    },
    worker::{WorkerDeploymentOptions, WorkerDeploymentVersion, WorkerTaskTypes},
};
use temporalio_sdk_core::{
    CoreRuntime, PollError, PollerBehavior, RuntimeOptions, Worker, WorkerConfig,
    WorkerVersioningStrategy, init_worker,
};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio_util::sync::CancellationToken;

const MAX_COMPLETION: i32 = 4 * 1024 * 1024;
static WORKERS: LazyLock<Mutex<HashMap<i64, CancellationToken>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

type LocalActivityResults = HashMap<i64, UnboundedSender<Vec<u8>>>;
static LOCAL_ACTIVITY_RESULTS: LazyLock<Mutex<LocalActivityResults>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

unsafe extern "C" {
    #[link_name = "jvm:static:poc/JavaHost:handleActivation"]
    fn handle_activation(context: i64, activation: *const u8, length: i32) -> i32;

    #[link_name = "jvm:static:poc/JavaHost:copyCompletion"]
    fn copy_completion(context: i64, destination: *mut u8, length: i32) -> bool;

    #[link_name = "jvm:static:poc/JavaHost:dispatchLocalActivity"]
    fn dispatch_local_activity(context: i64, task: *const u8, length: i32) -> bool;

    #[link_name = "jvm:static:poc/JavaHost:workerFailed"]
    fn worker_failed(context: i64, message: *const u8, length: i32);
}

unsafe fn borrowed_bytes<'a>(data: *const u8, length: i32) -> Option<&'a [u8]> {
    match length {
        0 => Some(&[]),
        1.. if !data.is_null() => {
            Some(unsafe { std::slice::from_raw_parts(data, length as usize) })
        }
        _ => None,
    }
}

/// # Safety
/// The Java caller keeps the given payload available until this call returns.
pub unsafe fn complete_rpc(token: i64, data: *const u8, length: i32) {
    transport::finish(
        token,
        unsafe { borrowed_bytes(data, length) }.unwrap_or_default(),
    );
}

pub fn register_worker(context: i64) -> bool {
    let mut map = WORKERS.lock().unwrap();
    if map.contains_key(&context) {
        return false;
    }
    map.insert(context, CancellationToken::new());
    true
}

pub fn shutdown_worker(context: i64) {
    if let Some(worker) = WORKERS.lock().unwrap().get(&context).cloned() {
        worker.cancel();
    }
}

/// # Safety
/// The Java caller keeps the options buffer available until this call returns.
pub unsafe fn run_worker(context: i64, config: *const u8, length: i32) {
    let stop = WORKERS.lock().unwrap().get(&context).cloned();
    let result = (|| {
        let stop = stop.ok_or_else(|| anyhow!("worker was not registered"))?;
        let mut config = unsafe { borrowed_bytes(config, length) }
            .ok_or_else(|| anyhow!("invalid worker options"))?;
        let namespace = std::str::from_utf8(transport::take_field(&mut config)?)?.to_owned();
        let task_queue = std::str::from_utf8(transport::take_field(&mut config)?)?.to_owned();
        let [local_activities, legacy_versioning, rest @ ..] = config else {
            bail!("missing worker options");
        };
        if *local_activities > 1 || *legacy_versioning > 1 {
            bail!("invalid worker options");
        }
        let versioning = worker_versioning(rest, *legacy_versioning != 0)?;
        let tokio = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()?;
        tokio.block_on(run(
            context,
            stop,
            namespace,
            task_queue,
            *local_activities != 0,
            versioning,
        ))
    })();
    WORKERS.lock().unwrap().remove(&context);
    LOCAL_ACTIVITY_RESULTS.lock().unwrap().remove(&context);
    if let Err(error) = result {
        let message = format!("{error:#}");
        unsafe { worker_failed(context, message.as_ptr(), message.len() as i32) };
    }
}

fn worker_versioning(mut options: &[u8], legacy: bool) -> anyhow::Result<WorkerVersioningStrategy> {
    let build_id = std::str::from_utf8(transport::take_field(&mut options)?)?.to_owned();
    let deployment = transport::take_field(&mut options)?;
    let behavior = VersioningBehavior::try_from(i32::from_be_bytes(options.try_into()?))?;
    if deployment.is_empty() {
        return Ok(if legacy {
            WorkerVersioningStrategy::LegacyBuildIdBased { build_id }
        } else {
            WorkerVersioningStrategy::None { build_id }
        });
    }

    let deployment = ProtoDeploymentOptions::decode(deployment)?;
    let is_versioned = deployment.worker_versioning_mode() == WorkerVersioningMode::Versioned;
    let version = WorkerDeploymentVersion::builder()
        .deployment_name(deployment.deployment_name)
        .build_id(deployment.build_id)
        .build();
    let default_behavior = match behavior {
        VersioningBehavior::Unspecified => None,
        behavior => Some(behavior.into()),
    };
    let options = WorkerDeploymentOptions::new(version)
        .use_worker_versioning(is_versioned)
        .maybe_default_versioning_behavior(default_behavior)
        .build();
    Ok(WorkerVersioningStrategy::WorkerDeploymentBased(options))
}

/// Enqueues a completion from the Java thread that ran the local activity.
pub unsafe fn complete_local_activity(context: i64, data: *const u8, length: i32) {
    let Some(bytes) = (unsafe { borrowed_bytes(data, length) }) else {
        return;
    };
    if let Some(sender) = LOCAL_ACTIVITY_RESULTS.lock().unwrap().get(&context) {
        let _ = sender.send(bytes.to_vec());
    }
}

pub fn forget_worker(context: i64) {
    WORKERS.lock().unwrap().remove(&context);
}

async fn connect(context: i64) -> anyhow::Result<Connection> {
    let connection_options =
        ConnectionOptions::new(temporalio_client::Url::parse("http://localhost")?)
            .identity(format!("sdk-java-poc-{context}"))
            .client_name("temporal-java-poc")
            .client_version(env!("CARGO_PKG_VERSION"))
            .dns_load_balancing(None)
            .service_override(CallbackBasedGrpcService {
                context,
                callback: transport::send,
            })
            .build();
    Ok(Connection::connect(connection_options).await?)
}

async fn run(
    context: i64,
    stop: CancellationToken,
    namespace: String,
    task_queue: String,
    local_activities: bool,
    versioning: WorkerVersioningStrategy,
) -> anyhow::Result<()> {
    let runtime = CoreRuntime::new_assume_tokio(
        RuntimeOptions::builder()
            .heartbeat_interval(None)
            .build()
            .map_err(|e| anyhow!(e))?,
    )?;
    let connection = tokio::select! {
        result = connect(context) => result.context("connecting through Java gRPC")?,
        _ = stop.cancelled() => return Ok(()),
    };
    let mut task_types = WorkerTaskTypes::workflow_only();
    task_types.enable_local_activities = local_activities;
    let config = WorkerConfig::builder()
        .namespace(namespace)
        .task_queue(task_queue)
        .task_types(task_types)
        .max_cached_workflows(2)
        .workflow_task_poller_behavior(PollerBehavior::SimpleMaximum(2))
        .max_outstanding_workflow_tasks(2usize)
        .versioning_strategy(versioning)
        .build()
        .map_err(|e| anyhow!(e))?;
    let worker =
        init_worker(&runtime, config, connection).context("initializing SDK Core worker")?;
    worker
        .validate()
        .await
        .context("validating SDK Core worker")?;
    let result = if local_activities {
        let (sender, results) = unbounded_channel();
        LOCAL_ACTIVITY_RESULTS
            .lock()
            .unwrap()
            .insert(context, sender);
        tokio::try_join!(
            poll_workflows(context, &worker, stop),
            poll_local_activities(context, &worker, results),
        )
        .map(|_| ())
    } else {
        poll_workflows(context, &worker, stop).await
    };
    worker.initiate_shutdown();
    result?;
    worker.finalize_shutdown().await;
    Ok(())
}

async fn poll_workflows(
    context: i64,
    worker: &Worker,
    stop: CancellationToken,
) -> anyhow::Result<()> {
    let mut shutting_down = false;
    loop {
        let activation = tokio::select! {
            _ = stop.cancelled(), if !shutting_down => {
                shutting_down = true;
                worker.initiate_shutdown();
                continue;
            }
            activation = worker.poll_workflow_activation() => activation,
        };
        let activation = match activation {
            Ok(activation) => activation,
            Err(PollError::ShutDown) => return Ok(()),
            Err(err) => return Err(anyhow!(err).context("polling SDK Core")),
        };
        let run_id = activation.run_id.clone();
        let completion = match activate(context, &activation.encode_to_vec(), &run_id) {
            Ok(completion) => completion,
            Err(err) => WorkflowActivationCompletion::fail(
                run_id,
                Failure {
                    message: format!("{err:#}"),
                    ..Default::default()
                },
                None,
            ),
        };
        worker
            .complete_workflow_activation(completion)
            .await
            .context("completing SDK Core activation")?;
    }
}

async fn poll_local_activities(
    context: i64,
    worker: &Worker,
    mut results: UnboundedReceiver<Vec<u8>>,
) -> anyhow::Result<()> {
    loop {
        tokio::select! {
            task = worker.poll_activity_task() => {
                let task = match task {
                    Ok(task) => task,
                    Err(PollError::ShutDown) => return Ok(()),
                    Err(err) => return Err(anyhow!(err).context("polling Core local activities")),
                };
                let bytes = task.encode_to_vec();
                if !unsafe { dispatch_local_activity(context, bytes.as_ptr(), i32::try_from(bytes.len())?) } {
                    bail!("Java failed to dispatch Core local activity task");
                }
            }
            completion = results.recv() => {
                let bytes = completion.ok_or_else(|| anyhow!("local activity completion channel closed"))?;
                let completion = ActivityTaskCompletion::decode(bytes.as_slice())
                    .context("decoding Java local activity completion")?;
                worker.complete_activity_task(completion).await
                    .context("completing Core local activity")?;
            }
        }
    }
}

fn activate(
    context: i64,
    bytes: &[u8],
    run_id: &str,
) -> anyhow::Result<WorkflowActivationCompletion> {
    let size = unsafe { handle_activation(context, bytes.as_ptr(), i32::try_from(bytes.len())?) };
    if !(0..=MAX_COMPLETION).contains(&size) {
        bail!("Java activation handler failed or returned an oversized completion");
    }
    let mut completion = vec![0; size as usize];
    if !unsafe { copy_completion(context, completion.as_mut_ptr(), size) } {
        bail!("Java activation completion disappeared");
    }
    let completion = WorkflowActivationCompletion::decode(completion.as_slice())?;
    if completion.run_id != run_id || completion.status.is_none() {
        bail!("Java returned a completion without matching run ID and status");
    }
    Ok(completion)
}
