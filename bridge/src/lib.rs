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
        coresdk::workflow_completion::WorkflowActivationCompletion,
        temporal::api::failure::v1::Failure,
    },
    worker::WorkerTaskTypes,
};
use temporalio_sdk_core::{
    CoreRuntime, PollError, PollerBehavior, RuntimeOptions, WorkerConfig, WorkerVersioningStrategy,
    init_worker,
};
use tokio_util::sync::CancellationToken;

const MAX_COMPLETION: i32 = 4 * 1024 * 1024;
static WORKERS: LazyLock<Mutex<HashMap<i64, CancellationToken>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

unsafe extern "C" {
    #[link_name = "jvm:static:poc/JavaHost:handleActivation"]
    fn handle_activation(context: i64, activation: *const u8, length: i32) -> i32;

    #[link_name = "jvm:static:poc/JavaHost:copyCompletion"]
    fn copy_completion(context: i64, destination: *mut u8, length: i32) -> bool;

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
        let tokio = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()?;
        tokio.block_on(run(context, stop, namespace, task_queue))
    })();
    WORKERS.lock().unwrap().remove(&context);
    if let Err(error) = result {
        let message = format!("{error:#}");
        unsafe { worker_failed(context, message.as_ptr(), message.len() as i32) };
    }
}

pub fn forget_worker(context: i64) {
    WORKERS.lock().unwrap().remove(&context);
}

pub fn probe_transport(context: i64) {
    let result = (|| {
        let tokio = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()?;
        tokio.block_on(async {
            let connection =
                tokio::time::timeout(std::time::Duration::from_secs(10), connect(context))
                    .await??;
            anyhow::ensure!(
                connection.capabilities().is_some(),
                "Temporal returned no capabilities"
            );
            Ok::<_, anyhow::Error>(())
        })
    })();
    if let Err(error) = result {
        let message = format!("{error:#}");
        unsafe { worker_failed(context, message.as_ptr(), message.len() as i32) };
    }
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
    let config = WorkerConfig::builder()
        .namespace(namespace)
        .task_queue(task_queue)
        .task_types(WorkerTaskTypes::workflow_only())
        .max_cached_workflows(2)
        .workflow_task_poller_behavior(PollerBehavior::SimpleMaximum(2))
        .max_outstanding_workflow_tasks(2usize)
        .versioning_strategy(WorkerVersioningStrategy::None {
            build_id: "sdk-java-poc".to_owned(),
        })
        .build()
        .map_err(|e| anyhow!(e))?;
    let worker =
        init_worker(&runtime, config, connection).context("initializing SDK Core worker")?;
    worker
        .validate()
        .await
        .context("validating SDK Core worker")?;
    let mut shutting_down = false;
    let mut failure = None;
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
            Err(PollError::ShutDown) => break,
            Err(err) => {
                failure = Some(anyhow!(err).context("polling SDK Core"));
                break;
            }
        };
        let run_id = activation.run_id.clone();
        let completion = if activation.is_only_eviction() {
            WorkflowActivationCompletion::empty(run_id)
        } else {
            match activate(context, &activation.encode_to_vec(), &run_id) {
                Ok(completion) => completion,
                Err(err) => WorkflowActivationCompletion::fail(
                    run_id,
                    Failure {
                        message: format!("{err:#}"),
                        ..Default::default()
                    },
                    None,
                ),
            }
        };
        if let Err(err) = worker.complete_workflow_activation(completion).await {
            failure = Some(anyhow!(err).context("completing SDK Core activation"));
            break;
        }
    }
    worker.initiate_shutdown();
    if let Some(err) = failure {
        return Err(err);
    }
    worker.finalize_shutdown().await;
    Ok(())
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
