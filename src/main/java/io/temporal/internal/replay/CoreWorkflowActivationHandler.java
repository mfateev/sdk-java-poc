package io.temporal.internal.replay;

import coresdk.workflow_activation.WorkflowActivationOuterClass.InitializeWorkflow;
import coresdk.workflow_activation.WorkflowActivationOuterClass.QueryWorkflow;
import coresdk.workflow_activation.WorkflowActivationOuterClass.WorkflowActivation;
import coresdk.workflow_activation.WorkflowActivationOuterClass.WorkflowActivationJob;
import coresdk.workflow_completion.WorkflowCompletion;
import coresdk.workflow_completion.WorkflowCompletion.Success;
import coresdk.workflow_completion.WorkflowCompletion.WorkflowActivationCompletion;
import io.temporal.api.common.v1.Header;
import io.temporal.api.common.v1.Payload;
import io.temporal.api.common.v1.Payloads;
import io.temporal.api.common.v1.WorkflowExecution;
import io.temporal.api.enums.v1.EventType;
import io.temporal.api.history.v1.HistoryEvent;
import io.temporal.api.query.v1.WorkflowQuery;
import io.temporal.internal.worker.SingleWorkerOptions;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Drives the existing Java workflow executor from Core's ordered, per-run activations. */
public final class CoreWorkflowActivationHandler implements AutoCloseable {
  private static final Logger log = LoggerFactory.getLogger(CoreWorkflowActivationHandler.class);
  private final Map<String, Run> runs = new HashMap<>();
  private final ReplayWorkflowFactory workflowFactory;
  private final SingleWorkerOptions options;
  private final String namespace;
  private final String taskQueue;

  public CoreWorkflowActivationHandler(
      ReplayWorkflowFactory workflowFactory,
      SingleWorkerOptions options,
      String namespace,
      String taskQueue) {
    this.workflowFactory = workflowFactory;
    this.options = options;
    this.namespace = namespace;
    this.taskQueue = taskQueue;
  }

  public byte[] activate(byte[] bytes) throws IOException {
    WorkflowActivation activation = WorkflowActivation.parseFrom(bytes);
    String runId = activation.getRunId();
    try {
      return apply(activation, runId).toByteArray();
    } catch (Exception | Error failure) {
      log.warn("Core workflow activation failed for run {}", runId, failure);
      return WorkflowActivationCompletion.newBuilder()
          .setRunId(runId)
          .setFailed(
              WorkflowCompletion.Failure.newBuilder()
                  .setFailure(options.getDataConverter().exceptionToFailure(failure)))
          .build()
          .toByteArray();
    }
  }

  private WorkflowActivationCompletion apply(WorkflowActivation activation, String runId)
      throws Exception {
    if (activation.getJobsCount() == 1 && activation.getJobs(0).hasRemoveFromCache()) {
      Run removed = runs.remove(runId);
      if (removed != null) {
        removed.executor.close();
      }
      return WorkflowActivationCompletion.newBuilder()
          .setRunId(runId)
          .setSuccessful(Success.getDefaultInstance())
          .build();
    }

    Run run = runs.get(runId);
    if (activation.getJobsCount() > 0 && activation.getJobs(0).hasInitializeWorkflow()) {
      if (run != null) {
        throw new IllegalStateException("Run already initialized: " + runId);
      }
      run = start(activation, activation.getJobs(0).getInitializeWorkflow());
    } else {
      if (run == null) {
        throw new IllegalStateException("Activation has no Java workflow for run " + runId);
      }
      run.context.beginActivation(activation);
    }

    boolean queryOnly = activation.getJobsCount() > 0 && activation.getJobs(0).hasQueryWorkflow();
    for (WorkflowActivationJob job : activation.getJobsList()) {
      applyJob(run, job);
    }
    if (!queryOnly) {
      run.executor.eventLoop();
    }
    Throwable failure = run.context.getWorkflowTaskFailure();
    if (failure != null) {
      throw new IllegalStateException("Java workflow task failed", failure);
    }
    return run.context.finishActivation();
  }

  private Run start(WorkflowActivation activation, InitializeWorkflow initialize) throws Exception {
    var attributes = CoreReplayWorkflowContext.startAttributes(initialize, taskQueue);
    var execution =
        WorkflowExecution.newBuilder()
            .setWorkflowId(initialize.getWorkflowId())
            .setRunId(activation.getRunId())
            .build();
    ReplayWorkflow workflow = workflowFactory.getWorkflow(attributes.getWorkflowType(), execution);
    var context =
        new CoreReplayWorkflowContext(namespace, execution, attributes, initialize, options);
    var executor = new ReplayWorkflowExecutor(workflow, context, context);
    var run = new Run(context, executor);
    runs.put(execution.getRunId(), run);
    context.beginActivation(activation);
    executor.start(
        HistoryEvent.newBuilder()
            .setEventType(EventType.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED)
            .setEventTime(initialize.getStartTime())
            .setWorkflowExecutionStartedEventAttributes(attributes)
            .build());
    log.debug(
        "Initialized Java workflow run {} (replay={})",
        execution.getRunId(),
        activation.getIsReplaying());
    return run;
  }

  private void applyJob(Run run, WorkflowActivationJob job) {
    switch (job.getVariantCase()) {
      case INITIALIZE_WORKFLOW -> {
        // The run was initialized before applying the other jobs.
      }
      case FIRE_TIMER -> run.context.fireTimer(job.getFireTimer().getSeq());
      case UPDATE_RANDOM_SEED ->
          run.context.updateRandomSeed(job.getUpdateRandomSeed().getRandomnessSeed());
      case CANCEL_WORKFLOW -> run.executor.handleCancel(job.getCancelWorkflow().getReason());
      case SIGNAL_WORKFLOW -> {
        var signal = job.getSignalWorkflow();
        run.executor.handleSignal(
            signal.getSignalName(),
            payloads(signal.getInputList()),
            signal.getOriginatingEventId(),
            header(signal.getHeadersMap()));
      }
      case QUERY_WORKFLOW -> query(run, job.getQueryWorkflow());
      default ->
          throw new UnsupportedOperationException(
              "Core activation job is outside the worker POC: " + job.getVariantCase());
    }
  }

  private void query(Run run, QueryWorkflow job) {
    var query =
        WorkflowQuery.newBuilder()
            .setQueryType(job.getQueryType())
            .setHeader(header(job.getHeadersMap()));
    payloads(job.getArgumentsList()).ifPresent(query::setQueryArgs);
    Optional<Payloads> result;
    try {
      result = run.executor.query(query.build());
    } catch (RuntimeException e) {
      run.context.respondToQuery(
          job.getQueryId(), Optional.empty(), options.getDataConverter().exceptionToFailure(e));
      return;
    }
    run.context.respondToQuery(job.getQueryId(), result, null);
  }

  private static Optional<Payloads> payloads(List<Payload> values) {
    return values.isEmpty()
        ? Optional.empty()
        : Optional.of(Payloads.newBuilder().addAllPayloads(values).build());
  }

  private static Header header(Map<String, Payload> values) {
    return Header.newBuilder().putAllFields(values).build();
  }

  @Override
  public void close() {
    runs.values().forEach(run -> run.executor.close());
    runs.clear();
  }

  private record Run(CoreReplayWorkflowContext context, ReplayWorkflowExecutor executor) {}
}
