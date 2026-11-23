package io.temporal.internal.replay;

import com.google.protobuf.Empty;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import coresdk.child_workflow.ChildWorkflow;
import coresdk.common.Common.NamespacedWorkflowExecution;
import coresdk.common.Common.VersioningIntent;
import coresdk.nexus.Nexus;
import coresdk.workflow_activation.WorkflowActivationOuterClass.*;
import coresdk.workflow_commands.WorkflowCommands;
import io.temporal.api.command.v1.SignalExternalWorkflowExecutionCommandAttributes;
import io.temporal.api.common.v1.Payload;
import io.temporal.api.common.v1.Payloads;
import io.temporal.api.common.v1.WorkflowExecution;
import io.temporal.api.enums.v1.RetryState;
import io.temporal.api.failure.v1.Failure;
import io.temporal.client.WorkflowExecutionAlreadyStarted;
import io.temporal.failure.ChildWorkflowFailure;
import io.temporal.internal.common.ProtobufTimeUtils;
import io.temporal.internal.common.SdkFlag;
import io.temporal.internal.statemachines.ExecuteActivityParameters;
import io.temporal.internal.statemachines.ExecuteLocalActivityParameters;
import io.temporal.internal.statemachines.LocalActivityCallback;
import io.temporal.internal.statemachines.LocalActivityCallback.LocalActivityFailedException;
import io.temporal.internal.statemachines.StartChildWorkflowExecutionParameters;
import io.temporal.internal.statemachines.StartNexusOperationParameters;
import io.temporal.internal.statemachines.UnsupportedVersion;
import io.temporal.internal.statemachines.UpdateProtocolCallback;
import io.temporal.workflow.CancelExternalWorkflowException;
import io.temporal.workflow.Functions;
import io.temporal.workflow.Workflow;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

final class CoreWorkflowOperations {
  private static final String VERSION_PREFIX = "java-sdk-version/";
  private final CoreReplayWorkflowContext context;
  private final Map<Integer, Pending<Consumer<ResolveActivity>>> activities = new HashMap<>();
  private final Map<Integer, Pending<Child>> children = new HashMap<>();
  private final Map<Integer, Pending<NexusOperation>> nexus = new HashMap<>();
  private final Map<Integer, Pending<Functions.Proc2<Void, Failure>>> signals = new HashMap<>();
  private final Map<Integer, CancelRequest> cancelRequests = new HashMap<>();
  private final Map<String, Integer> versions = new HashMap<>();
  private final Map<String, Integer> recordedVersions = new HashMap<>();
  private int nextSequence;

  CoreWorkflowOperations(CoreReplayWorkflowContext context) {
    this.context = context;
  }

  ReplayWorkflowContext.ScheduleActivityTaskOutput scheduleActivity(
      ExecuteActivityParameters parameters, Functions.Proc2<Optional<Payloads>, Failure> callback) {
    var attributes = parameters.getAttributes();
    String id =
        attributes.getActivityId().isEmpty()
            ? context.randomUUID().toString()
            : attributes.getActivityId();
    int seq = ++nextSequence;
    var activity =
        WorkflowCommands.ScheduleActivity.newBuilder()
            .setSeq(seq)
            .setActivityId(id)
            .setActivityType(attributes.getActivityType().getName())
            .setTaskQueue(attributes.getTaskQueue().getName())
            .putAllHeaders(attributes.getHeader().getFieldsMap())
            .addAllArguments(attributes.getInput().getPayloadsList())
            // The existing Java activity worker handles remote activities, including heartbeats.
            .setDoNotEagerlyExecute(true)
            .setCancellationType(
                WorkflowCommands.ActivityCancellationType.valueOf(
                    parameters.getCancellationType().name()));
    if (attributes.hasScheduleToCloseTimeout())
      activity.setScheduleToCloseTimeout(attributes.getScheduleToCloseTimeout());
    if (attributes.hasScheduleToStartTimeout())
      activity.setScheduleToStartTimeout(attributes.getScheduleToStartTimeout());
    if (attributes.hasStartToCloseTimeout())
      activity.setStartToCloseTimeout(attributes.getStartToCloseTimeout());
    if (attributes.hasHeartbeatTimeout())
      activity.setHeartbeatTimeout(attributes.getHeartbeatTimeout());
    if (attributes.hasRetryPolicy()) activity.setRetryPolicy(attributes.getRetryPolicy());
    if (attributes.hasPriority()) activity.setPriority(attributes.getPriority());
    if (attributes.getUseWorkflowBuildId())
      activity.setVersioningIntent(VersioningIntent.COMPATIBLE);
    activities.put(
        seq,
        new Pending<>(
            job -> {
              var result = job.getResult();
              switch (result.getStatusCase()) {
                case COMPLETED ->
                    callback.apply(
                        payloads(
                            result.getCompleted().hasResult(), result.getCompleted().getResult()),
                        null);
                case FAILED -> callback.apply(Optional.empty(), result.getFailed().getFailure());
                case CANCELLED ->
                    callback.apply(Optional.empty(), result.getCancelled().getFailure());
                default -> throw unexpected("remote activity resolution", result.getStatusCase());
              }
            }));
    context.addCommand(
        WorkflowCommands.WorkflowCommand.newBuilder().setScheduleActivity(activity),
        parameters.getMetadata());
    var cancel =
        WorkflowCommands.WorkflowCommand.newBuilder()
            .setRequestCancelActivity(
                WorkflowCommands.RequestCancelActivity.newBuilder().setSeq(seq));
    return new ReplayWorkflowContext.ScheduleActivityTaskOutput(
        id, ignored -> cancel(activities, seq, cancel));
  }

  Functions.Proc scheduleLocalActivity(
      ExecuteLocalActivityParameters parameters, LocalActivityCallback callback) {
    var attributes = parameters.cloneActivityTaskBuilder();
    int seq = ++nextSequence;
    var activity =
        WorkflowCommands.ScheduleLocalActivity.newBuilder()
            .setSeq(seq)
            .setActivityId(attributes.getActivityId())
            .setActivityType(attributes.getActivityType().getName())
            .setAttempt(parameters.getInitialAttempt())
            .putAllHeaders(attributes.getHeader().getFieldsMap())
            .addAllArguments(attributes.getInput().getPayloadsList())
            .setLocalRetryThreshold(
                ProtobufTimeUtils.toProtoDuration(parameters.getLocalRetryThreshold()))
            .setCancellationType(
                WorkflowCommands.ActivityCancellationType.WAIT_CANCELLATION_COMPLETED)
            .setIncludeArgumentsInMarker(!parameters.isDoNotIncludeArgumentsIntoMarker());
    if (attributes.hasScheduleToCloseTimeout())
      activity.setScheduleToCloseTimeout(attributes.getScheduleToCloseTimeout());
    if (attributes.hasStartToCloseTimeout())
      activity.setStartToCloseTimeout(attributes.getStartToCloseTimeout());
    if (attributes.hasRetryPolicy()) activity.setRetryPolicy(attributes.getRetryPolicy());
    if (parameters.getScheduleToStartTimeout() != null) {
      activity.setScheduleToStartTimeout(
          ProtobufTimeUtils.toProtoDuration(parameters.getScheduleToStartTimeout()));
    }
    if (parameters.getInitialAttempt() > 1) {
      try {
        activity.setOriginalScheduleTime(
            Timestamp.parseFrom(
                parameters.getPreviousLocalExecutionFailure().getEncodedAttributes().getData()));
      } catch (InvalidProtocolBufferException e) {
        throw new IllegalStateException("Invalid Core local activity retry timestamp", e);
      }
    }
    activities.put(
        seq,
        new Pending<>(
            job -> {
              var result = job.getResult();
              switch (result.getStatusCase()) {
                case COMPLETED ->
                    callback.apply(
                        payloads(
                            result.getCompleted().hasResult(), result.getCompleted().getResult()),
                        null);
                case FAILED ->
                    callback.apply(
                        Optional.empty(),
                        localFailure(parameters, result.getFailed().getFailure()));
                case CANCELLED ->
                    callback.apply(
                        Optional.empty(),
                        localFailure(parameters, result.getCancelled().getFailure()));
                case BACKOFF -> {
                  var backoff = result.getBackoff();
                  // The stock Java runner passes this value through to the next attempt.
                  var retry =
                      Failure.newBuilder()
                          .setMessage("Core local activity retry")
                          .setEncodedAttributes(
                              Payload.newBuilder()
                                  .setData(backoff.getOriginalScheduleTime().toByteString()))
                          .build();
                  callback.apply(
                      Optional.empty(),
                      new LocalActivityFailedException(
                          retry,
                          com.google.protobuf.util.Timestamps.toMillis(
                              backoff.getOriginalScheduleTime()),
                          backoff.getAttempt() - 1,
                          ProtobufTimeUtils.toJavaDuration(backoff.getBackoffDuration())));
                }
                default -> throw unexpected("local activity resolution", result.getStatusCase());
              }
            }));
    context.addCommand(
        WorkflowCommands.WorkflowCommand.newBuilder().setScheduleLocalActivity(activity),
        parameters.getMetadata());
    var cancel =
        WorkflowCommands.WorkflowCommand.newBuilder()
            .setRequestCancelLocalActivity(
                WorkflowCommands.RequestCancelLocalActivity.newBuilder().setSeq(seq));
    return () -> cancel(activities, seq, cancel);
  }

  private static LocalActivityFailedException localFailure(
      ExecuteLocalActivityParameters parameters, Failure failure) {
    return new LocalActivityFailedException(
        failure, parameters.getOriginalScheduledTimestamp(), parameters.getInitialAttempt(), null);
  }

  void resolveActivity(ResolveActivity job) {
    take(activities, job.getSeq(), "activity").value.accept(job);
  }

  Functions.Proc1<Exception> startChild(
      StartChildWorkflowExecutionParameters parameters,
      Functions.Proc2<WorkflowExecution, Exception> started,
      Functions.Proc2<Optional<Payloads>, Exception> completed) {
    var attributes = parameters.getRequest();
    String namespace =
        attributes.getNamespace().isEmpty() ? context.getNamespace() : attributes.getNamespace();
    int seq = ++nextSequence;
    var child =
        WorkflowCommands.StartChildWorkflowExecution.newBuilder()
            .setSeq(seq)
            .setNamespace(namespace)
            .setWorkflowId(attributes.getWorkflowId())
            .setWorkflowType(attributes.getWorkflowType().getName())
            .setTaskQueue(attributes.getTaskQueue().getName())
            .addAllInput(attributes.getInput().getPayloadsList())
            .setParentClosePolicy(
                ChildWorkflow.ParentClosePolicy.forNumber(attributes.getParentClosePolicyValue()))
            .setWorkflowIdReusePolicy(attributes.getWorkflowIdReusePolicy())
            .setCronSchedule(attributes.getCronSchedule())
            .putAllHeaders(attributes.getHeader().getFieldsMap())
            .putAllMemo(attributes.getMemo().getFieldsMap())
            .setCancellationType(
                ChildWorkflow.ChildWorkflowCancellationType.valueOf(
                    parameters.getCancellationType().name()));
    if (attributes.hasWorkflowExecutionTimeout())
      child.setWorkflowExecutionTimeout(attributes.getWorkflowExecutionTimeout());
    if (attributes.hasWorkflowRunTimeout())
      child.setWorkflowRunTimeout(attributes.getWorkflowRunTimeout());
    if (attributes.hasWorkflowTaskTimeout())
      child.setWorkflowTaskTimeout(attributes.getWorkflowTaskTimeout());
    if (attributes.hasRetryPolicy()) child.setRetryPolicy(attributes.getRetryPolicy());
    if (attributes.hasSearchAttributes())
      child.setSearchAttributes(attributes.getSearchAttributes());
    if (attributes.hasPriority()) child.setPriority(attributes.getPriority());
    if (attributes.hasVersioningOverride())
      child.setVersioningOverride(attributes.getVersioningOverride());
    if (attributes.getInheritBuildId()) child.setVersioningIntent(VersioningIntent.COMPATIBLE);
    children.put(
        seq,
        new Pending<>(
            new Child(
                namespace,
                attributes.getWorkflowId(),
                attributes.getWorkflowType().getName(),
                started,
                completed)));
    context.addCommand(
        WorkflowCommands.WorkflowCommand.newBuilder().setStartChildWorkflowExecution(child),
        parameters.getMetadata());
    return reason ->
        cancel(
            children,
            seq,
            WorkflowCommands.WorkflowCommand.newBuilder()
                .setCancelChildWorkflowExecution(
                    WorkflowCommands.CancelChildWorkflowExecution.newBuilder()
                        .setChildWorkflowSeq(seq)
                        .setReason(
                            reason == null || reason.getMessage() == null
                                ? ""
                                : reason.getMessage())));
  }

  void resolveChildStart(ResolveChildWorkflowExecutionStart job) {
    Child child = pending(children, job.getSeq(), "child start").value;
    switch (job.getStatusCase()) {
      case SUCCEEDED ->
          child.started.apply(
              WorkflowExecution.newBuilder()
                  .setWorkflowId(child.id)
                  .setRunId(job.getSucceeded().getRunId())
                  .build(),
              null);
      case FAILED, CANCELLED -> {
        children.remove(job.getSeq());
        RuntimeException failure;
        if (job.hasCancelled()) {
          failure = context.decodeFailure(job.getCancelled().getFailure(), child.id);
        } else {
          var execution = WorkflowExecution.newBuilder().setWorkflowId(child.id).build();
          var cause =
              switch (job.getFailed().getCause()) {
                case START_CHILD_WORKFLOW_EXECUTION_FAILED_CAUSE_WORKFLOW_ALREADY_EXISTS ->
                    new WorkflowExecutionAlreadyStarted(execution, child.type, null);
                default -> new IllegalStateException(job.getFailed().getCause().name());
              };
          failure =
              new ChildWorkflowFailure(
                  0,
                  0,
                  child.type,
                  execution,
                  child.namespace,
                  RetryState.RETRY_STATE_NON_RETRYABLE_FAILURE,
                  cause);
        }
        child.started.apply(null, failure);
        child.completed.apply(Optional.empty(), failure);
      }
      default -> throw unexpected("child start resolution", job.getStatusCase());
    }
  }

  void resolveChild(ResolveChildWorkflowExecution job) {
    Child child = take(children, job.getSeq(), "child").value;
    var result = job.getResult();
    switch (result.getStatusCase()) {
      case COMPLETED ->
          child.completed.apply(
              payloads(result.getCompleted().hasResult(), result.getCompleted().getResult()), null);
      case FAILED ->
          child.completed.apply(
              Optional.empty(), context.decodeFailure(result.getFailed().getFailure(), child.id));
      case CANCELLED ->
          child.completed.apply(
              Optional.empty(),
              context.decodeFailure(result.getCancelled().getFailure(), child.id));
      default -> throw unexpected("child resolution", result.getStatusCase());
    }
  }

  Functions.Proc1<Exception> signalExternal(
      SignalExternalWorkflowExecutionCommandAttributes.Builder attributes,
      Functions.Proc2<Void, Failure> callback) {
    int seq = ++nextSequence;
    var signal =
        WorkflowCommands.SignalExternalWorkflowExecution.newBuilder()
            .setSeq(seq)
            .setSignalName(attributes.getSignalName())
            .addAllArgs(attributes.getInput().getPayloadsList())
            .putAllHeaders(attributes.getHeader().getFieldsMap());
    if (attributes.getChildWorkflowOnly()) {
      signal.setChildWorkflowId(attributes.getExecution().getWorkflowId());
    } else {
      signal.setWorkflowExecution(target(attributes.getNamespace(), attributes.getExecution()));
    }
    signals.put(seq, new Pending<>(callback));
    context.addCommand(
        WorkflowCommands.WorkflowCommand.newBuilder().setSignalExternalWorkflowExecution(signal),
        null);
    var cancel =
        WorkflowCommands.WorkflowCommand.newBuilder()
            .setCancelSignalWorkflow(
                WorkflowCommands.CancelSignalWorkflow.newBuilder().setSeq(seq));
    return ignored -> cancel(signals, seq, cancel);
  }

  void resolveExternalSignal(ResolveSignalExternalWorkflow job) {
    take(signals, job.getSeq(), "external signal")
        .value
        .apply(null, job.hasFailure() ? job.getFailure() : null);
  }

  void requestExternalCancel(
      WorkflowExecution execution,
      String reason,
      Functions.Proc2<Void, RuntimeException> callback) {
    int seq = ++nextSequence;
    var cancel =
        WorkflowCommands.RequestCancelExternalWorkflowExecution.newBuilder()
            .setSeq(seq)
            .setWorkflowExecution(target(context.getNamespace(), execution));
    if (reason != null) cancel.setReason(reason);
    cancelRequests.put(seq, new CancelRequest(execution, callback));
    context.addCommand(
        WorkflowCommands.WorkflowCommand.newBuilder()
            .setRequestCancelExternalWorkflowExecution(cancel),
        null);
  }

  void resolveExternalCancel(ResolveRequestCancelExternalWorkflow job) {
    CancelRequest request = cancelRequests.remove(job.getSeq());
    if (request == null) throw unexpected("external cancellation sequence", job.getSeq());
    request.callback.apply(
        null,
        job.hasFailure()
            ? new CancelExternalWorkflowException(
                job.getCause().name(),
                request.execution,
                null,
                context.decodeFailure(job.getFailure(), request.execution.getWorkflowId()))
            : null);
  }

  private NamespacedWorkflowExecution target(String namespace, WorkflowExecution execution) {
    return NamespacedWorkflowExecution.newBuilder()
        .setNamespace(namespace.isEmpty() ? context.getNamespace() : namespace)
        .setWorkflowId(execution.getWorkflowId())
        .setRunId(execution.getRunId())
        .build();
  }

  Functions.Proc1<Exception> startNexus(
      StartNexusOperationParameters parameters,
      Functions.Proc2<Optional<String>, Failure> started,
      Functions.Proc2<Optional<Payload>, Failure> completed) {
    var attributes = parameters.getAttributes();
    int seq = ++nextSequence;
    var operation =
        WorkflowCommands.ScheduleNexusOperation.newBuilder()
            .setSeq(seq)
            .setEndpoint(attributes.getEndpoint())
            .setService(attributes.getService())
            .setOperation(attributes.getOperation())
            .putAllNexusHeader(attributes.getNexusHeaderMap());
    if (parameters.getCancellationType() != null) {
      operation.setCancellationType(
          switch (parameters.getCancellationType()) {
            case WAIT_COMPLETED -> Nexus.NexusOperationCancellationType.WAIT_CANCELLATION_COMPLETED;
            case WAIT_REQUESTED -> Nexus.NexusOperationCancellationType.WAIT_CANCELLATION_REQUESTED;
            case TRY_CANCEL -> Nexus.NexusOperationCancellationType.TRY_CANCEL;
            case ABANDON -> Nexus.NexusOperationCancellationType.ABANDON;
          });
    }
    if (attributes.hasInput()) operation.setInput(attributes.getInput());
    if (attributes.hasScheduleToCloseTimeout())
      operation.setScheduleToCloseTimeout(attributes.getScheduleToCloseTimeout());
    if (attributes.hasScheduleToStartTimeout())
      operation.setScheduleToStartTimeout(attributes.getScheduleToStartTimeout());
    if (attributes.hasStartToCloseTimeout())
      operation.setStartToCloseTimeout(attributes.getStartToCloseTimeout());
    nexus.put(seq, new Pending<>(new NexusOperation(started, completed)));
    context.addCommand(
        WorkflowCommands.WorkflowCommand.newBuilder().setScheduleNexusOperation(operation),
        parameters.getMetadata());
    var cancel =
        WorkflowCommands.WorkflowCommand.newBuilder()
            .setRequestCancelNexusOperation(
                WorkflowCommands.RequestCancelNexusOperation.newBuilder().setSeq(seq));
    return ignored -> cancel(nexus, seq, cancel);
  }

  void resolveNexusStart(ResolveNexusOperationStart job) {
    NexusOperation operation = pending(nexus, job.getSeq(), "Nexus start").value;
    switch (job.getStatusCase()) {
      case OPERATION_TOKEN -> operation.started.apply(Optional.of(job.getOperationToken()), null);
      case STARTED_SYNC -> operation.started.apply(Optional.empty(), null);
      case FAILED -> {
        nexus.remove(job.getSeq());
        operation.started.apply(Optional.empty(), job.getFailed());
        operation.completed.apply(Optional.empty(), job.getFailed());
      }
      default -> throw unexpected("Nexus start resolution", job.getStatusCase());
    }
  }

  void resolveNexus(ResolveNexusOperation job) {
    NexusOperation operation = take(nexus, job.getSeq(), "Nexus operation").value;
    var result = job.getResult();
    switch (result.getStatusCase()) {
      case COMPLETED -> operation.completed.apply(Optional.of(result.getCompleted()), null);
      case FAILED -> operation.completed.apply(Optional.empty(), result.getFailed());
      case CANCELLED -> operation.completed.apply(Optional.empty(), result.getCancelled());
      case TIMED_OUT -> operation.completed.apply(Optional.empty(), result.getTimedOut());
      default -> throw unexpected("Nexus resolution", result.getStatusCase());
    }
  }

  UpdateProtocolCallback updateCallback(DoUpdate job) {
    return new UpdateProtocolCallback() {
      @Override
      public void accept() {
        respond(
            WorkflowCommands.UpdateResponse.newBuilder().setAccepted(Empty.getDefaultInstance()));
      }

      @Override
      public void reject(Failure failure) {
        respond(WorkflowCommands.UpdateResponse.newBuilder().setRejected(failure));
      }

      @Override
      public void complete(Optional<Payloads> result, Failure failure) {
        if (failure != null) {
          reject(failure);
        } else {
          respond(
              WorkflowCommands.UpdateResponse.newBuilder()
                  .setCompleted(
                      result
                          .map(CoreReplayWorkflowContext::singlePayload)
                          .orElseGet(Payload::getDefaultInstance)));
        }
      }

      @Override
      public boolean isReplaying() {
        return !job.getRunValidator();
      }

      private void respond(WorkflowCommands.UpdateResponse.Builder response) {
        context.addCommand(
            WorkflowCommands.WorkflowCommand.newBuilder()
                .setUpdateResponse(response.setProtocolInstanceId(job.getProtocolInstanceId())),
            null);
      }
    };
  }

  void notifyPatch(String patch) {
    if (!patch.startsWith(VERSION_PREFIX)) return;
    int delimiter = patch.lastIndexOf('/');
    String changeId =
        new String(
            Base64.getUrlDecoder().decode(patch.substring(VERSION_PREFIX.length(), delimiter)),
            StandardCharsets.UTF_8);
    int version = Integer.parseInt(patch.substring(delimiter + 1));
    Integer previous = recordedVersions.putIfAbsent(changeId, version);
    if (previous != null && previous != version)
      throw unexpected("versions for " + changeId, patch);
  }

  Integer getVersion(
      String changeId, int min, int max, Functions.Proc2<Integer, RuntimeException> callback) {
    boolean absentFromReplay =
        context.isReplaying()
            && !versions.containsKey(changeId)
            && !recordedVersions.containsKey(changeId);
    context.tryUseSdkFlag(SdkFlag.SKIP_YIELD_ON_VERSION);
    int version =
        versions.computeIfAbsent(
            changeId,
            id -> {
              int selected =
                  context.isReplaying()
                      ? recordedVersions.getOrDefault(id, Workflow.DEFAULT_VERSION)
                      : max;
              if (selected != Workflow.DEFAULT_VERSION) {
                String patch =
                    VERSION_PREFIX
                        + Base64.getUrlEncoder()
                            .withoutPadding()
                            .encodeToString(id.getBytes(StandardCharsets.UTF_8))
                        + "/"
                        + selected;
                context.addCommand(
                    WorkflowCommands.WorkflowCommand.newBuilder()
                        .setSetPatchMarker(
                            WorkflowCommands.SetPatchMarker.newBuilder().setPatchId(patch)),
                    null);
              }
              return selected;
            });
    if (version < min || version > max) {
      callback.apply(
          null,
          new UnsupportedVersion.UnsupportedVersionException(
              "Version "
                  + version
                  + " for "
                  + changeId
                  + " is outside ["
                  + min
                  + ", "
                  + max
                  + "]"));
    } else {
      callback.apply(version, null);
    }
    // The Java runner uses null to recognize a getVersion call introduced after this history.
    return absentFromReplay ? null : version;
  }

  private void cancel(
      Map<Integer, ? extends Pending<?>> entries,
      int seq,
      WorkflowCommands.WorkflowCommand.Builder command) {
    Pending<?> entry = entries.get(seq);
    if (entry != null && !entry.cancellationRequested) {
      entry.cancellationRequested = true;
      context.addCommand(command, null);
    }
  }

  private static <T> Pending<T> pending(
      Map<Integer, Pending<T>> entries, int seq, String operation) {
    Pending<T> entry = entries.get(seq);
    if (entry == null) throw unexpected(operation + " sequence", seq);
    return entry;
  }

  private static <T> Pending<T> take(Map<Integer, Pending<T>> entries, int seq, String operation) {
    Pending<T> entry = entries.remove(seq);
    if (entry == null) throw unexpected(operation + " sequence", seq);
    return entry;
  }

  private static IllegalStateException unexpected(String operation, Object value) {
    return new IllegalStateException("Unexpected " + operation + " " + value);
  }

  private static Optional<Payloads> payloads(boolean present, Payload payload) {
    return present
        ? Optional.of(Payloads.newBuilder().addPayloads(payload).build())
        : Optional.empty();
  }

  private static final class Pending<T> {
    private final T value;
    private boolean cancellationRequested;

    private Pending(T value) {
      this.value = value;
    }
  }

  private record Child(
      String namespace,
      String id,
      String type,
      Functions.Proc2<WorkflowExecution, Exception> started,
      Functions.Proc2<Optional<Payloads>, Exception> completed) {}

  private record CancelRequest(
      WorkflowExecution execution, Functions.Proc2<Void, RuntimeException> callback) {}

  private record NexusOperation(
      Functions.Proc2<Optional<String>, Failure> started,
      Functions.Proc2<Optional<Payload>, Failure> completed) {}
}
