package io.temporal.internal.replay;

import com.google.protobuf.util.Timestamps;
import com.uber.m3.tally.Scope;
import coresdk.workflow_activation.WorkflowActivationOuterClass.InitializeWorkflow;
import coresdk.workflow_activation.WorkflowActivationOuterClass.WorkflowActivation;
import coresdk.workflow_commands.WorkflowCommands.CancelTimer;
import coresdk.workflow_commands.WorkflowCommands.CancelWorkflowExecution;
import coresdk.workflow_commands.WorkflowCommands.CompleteWorkflowExecution;
import coresdk.workflow_commands.WorkflowCommands.FailWorkflowExecution;
import coresdk.workflow_commands.WorkflowCommands.ModifyWorkflowProperties;
import coresdk.workflow_commands.WorkflowCommands.QueryResult;
import coresdk.workflow_commands.WorkflowCommands.QuerySuccess;
import coresdk.workflow_commands.WorkflowCommands.StartTimer;
import coresdk.workflow_commands.WorkflowCommands.UpsertWorkflowSearchAttributes;
import coresdk.workflow_commands.WorkflowCommands.WorkflowCommand;
import coresdk.workflow_completion.WorkflowCompletion.Success;
import coresdk.workflow_completion.WorkflowCompletion.WorkflowActivationCompletion;
import io.temporal.api.command.v1.ContinueAsNewWorkflowExecutionCommandAttributes;
import io.temporal.api.command.v1.SignalExternalWorkflowExecutionCommandAttributes;
import io.temporal.api.common.v1.*;
import io.temporal.api.failure.v1.Failure;
import io.temporal.api.history.v1.WorkflowExecutionStartedEventAttributes;
import io.temporal.api.sdk.v1.UserMetadata;
import io.temporal.api.taskqueue.v1.TaskQueue;
import io.temporal.common.RetryOptions;
import io.temporal.failure.CanceledFailure;
import io.temporal.internal.common.ProtobufTimeUtils;
import io.temporal.internal.common.SdkFlag;
import io.temporal.internal.statemachines.*;
import io.temporal.internal.worker.SingleWorkerOptions;
import io.temporal.workflow.Functions;
import io.temporal.workflow.Functions.Func;
import io.temporal.workflow.Functions.Func1;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.TreeSet;
import java.util.UUID;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Adapts operations from the Java workflow runner to the Core activation protocol. */
final class CoreReplayWorkflowContext
    extends ReplayWorkflowExecutor.CompletionSink implements ReplayWorkflowContext {
  private final BasicWorkflowContext basicWorkflowContext;
  private final WorkflowMutableState mutableState;
  private final SingleWorkerOptions workerOptions;
  private final Scope replayAwareWorkflowMetricsScope;
  private final Map<Integer, Functions.Proc1<RuntimeException>> timers = new HashMap<>();
  private final TreeSet<Integer> usedFlags = new TreeSet<>();
  private final Random random;
  private WorkflowActivation activation;
  private Success.Builder commands;
  private int nextTimerSequence;

  CoreReplayWorkflowContext(
      String namespace,
      WorkflowExecution execution,
      WorkflowExecutionStartedEventAttributes startedAttributes,
      InitializeWorkflow initialize,
      SingleWorkerOptions workerOptions) {
    this.workerOptions = workerOptions;
    basicWorkflowContext =
        new BasicWorkflowContext(
            namespace,
            execution,
            startedAttributes,
            Timestamps.toMillis(initialize.getStartTime()));
    mutableState = new WorkflowMutableState(startedAttributes);
    replayAwareWorkflowMetricsScope =
        new ReplayAwareScope(workerOptions.getMetricsScope(), this, this::currentTimeMillis);
    random = new Random(initialize.getRandomnessSeed());
  }

  /** Keeps the existing SyncWorkflow start API and its constructor/entry-point ordering. */
  static WorkflowExecutionStartedEventAttributes startAttributes(
      InitializeWorkflow init, String taskQueue) {
    var attributes =
        WorkflowExecutionStartedEventAttributes.newBuilder()
            .setWorkflowType(WorkflowType.newBuilder().setName(init.getWorkflowType()))
            .setWorkflowId(init.getWorkflowId())
            .setTaskQueue(TaskQueue.newBuilder().setName(taskQueue))
            .setContinuedExecutionRunId(init.getContinuedFromExecutionRunId())
            .setFirstExecutionRunId(init.getFirstExecutionRunId())
            .setOriginalExecutionRunId(init.getOriginalExecutionRunId())
            .setAttempt(init.getAttempt())
            .setCronSchedule(init.getCronSchedule())
            .setIdentity(init.getIdentity())
            .setInitiator(init.getContinuedInitiator());
    if (init.getArgumentsCount() != 0) {
      attributes.setInput(Payloads.newBuilder().addAllPayloads(init.getArgumentsList()));
    }
    if (!init.getHeadersMap().isEmpty()) {
      attributes.setHeader(Header.newBuilder().putAllFields(init.getHeadersMap()));
    }
    if (init.hasParentWorkflowInfo()) {
      var parent = init.getParentWorkflowInfo();
      attributes.setParentWorkflowNamespace(parent.getNamespace());
      attributes.setParentWorkflowExecution(
          WorkflowExecution.newBuilder()
              .setWorkflowId(parent.getWorkflowId())
              .setRunId(parent.getRunId()));
    }
    if (init.hasRootWorkflow()) {
      attributes.setRootWorkflowExecution(init.getRootWorkflow());
    }
    if (init.hasWorkflowExecutionTimeout()) {
      attributes.setWorkflowExecutionTimeout(init.getWorkflowExecutionTimeout());
    }
    if (init.hasWorkflowRunTimeout()) {
      attributes.setWorkflowRunTimeout(init.getWorkflowRunTimeout());
    }
    if (init.hasWorkflowTaskTimeout()) {
      attributes.setWorkflowTaskTimeout(init.getWorkflowTaskTimeout());
    }
    if (init.hasWorkflowExecutionExpirationTime()) {
      attributes.setWorkflowExecutionExpirationTime(init.getWorkflowExecutionExpirationTime());
    }
    if (init.hasRetryPolicy()) {
      attributes.setRetryPolicy(init.getRetryPolicy());
    }
    if (init.hasLastCompletionResult()) {
      attributes.setLastCompletionResult(init.getLastCompletionResult());
    }
    if (init.hasContinuedFailure()) {
      attributes.setContinuedFailure(init.getContinuedFailure());
    }
    if (init.hasMemo()) {
      attributes.setMemo(init.getMemo());
    }
    if (init.hasSearchAttributes()) {
      attributes.setSearchAttributes(init.getSearchAttributes());
    }
    if (init.hasPriority()) {
      attributes.setPriority(init.getPriority());
    }
    return attributes.build();
  }

  void beginActivation(WorkflowActivation activation) {
    this.activation = activation;
    this.commands = Success.newBuilder();
    this.usedFlags.clear();
  }

  WorkflowActivationCompletion finishActivation() {
    commands.addAllUsedInternalFlags(usedFlags);
    return WorkflowActivationCompletion.newBuilder()
        .setRunId(getRunId())
        .setSuccessful(commands)
        .build();
  }

  @Override
  public Functions.Proc1<RuntimeException> newTimer(
      Duration delay, UserMetadata metadata, Functions.Proc1<RuntimeException> callback) {
    if (delay.isNegative() || delay.isZero()) {
      callback.apply(null);
      return ignored -> {};
    }
    int sequence = ++nextTimerSequence;
    timers.put(sequence, callback);
    var command =
        WorkflowCommand.newBuilder()
            .setStartTimer(
                StartTimer.newBuilder()
                    .setSeq(sequence)
                    .setStartToFireTimeout(ProtobufTimeUtils.toProtoDuration(delay)));
    if (metadata != null) {
      command.setUserMetadata(metadata);
    }
    commands.addCommands(command);
    return ignored -> {
      if (timers.remove(sequence) != null) {
        commands.addCommands(
            WorkflowCommand.newBuilder().setCancelTimer(CancelTimer.newBuilder().setSeq(sequence)));
        callback.apply(new CanceledFailure("Canceled by request"));
      }
    };
  }

  void fireTimer(int sequence) {
    Functions.Proc1<RuntimeException> callback = timers.remove(sequence);
    if (callback == null) {
      throw new IllegalStateException("No pending timer for sequence " + sequence);
    }
    callback.apply(null);
  }

  @Override
  public long currentTimeMillis() {
    return Timestamps.toMillis(activation.getTimestamp());
  }

  @Override
  public boolean isReplaying() {
    return activation.getIsReplaying();
  }

  @Override
  public long getLastWorkflowTaskStartedEventId() {
    return Integer.toUnsignedLong(activation.getHistoryLength());
  }

  @Override
  public long getHistorySize() {
    return activation.getHistorySizeBytes();
  }

  @Override
  public boolean isContinueAsNewSuggested() {
    return activation.getContinueAsNewSuggested();
  }

  @Nonnull
  @Override
  public String getRunId() {
    return getWorkflowExecution().getRunId();
  }

  @Override
  public String getFullReplayDirectQueryName() {
    // Core exposes queries through activations. This flag is for the Java legacy replay path.
    return null;
  }

  @Override
  public Optional<String> getCurrentBuildId() {
    String buildId = activation.getDeploymentVersionForCurrentTask().getBuildId();
    return buildId.isEmpty() ? Optional.empty() : Optional.of(buildId);
  }

  void updateRandomSeed(long seed) {
    random.setSeed(seed);
  }

  @Override
  public UUID randomUUID() {
    return new UUID(random.nextLong(), random.nextLong());
  }

  @Override
  public Random newRandom() {
    return new Random(random.nextLong());
  }

  @Override
  public boolean tryUseSdkFlag(SdkFlag flag) {
    if (isReplaying()) {
      return checkSdkFlag(flag);
    }
    usedFlags.add(flag.getValue());
    return true;
  }

  @Override
  public boolean checkSdkFlag(SdkFlag flag) {
    return usedFlags.contains(flag.getValue())
        || activation.getAvailableInternalFlagsList().contains(flag.getValue());
  }

  @Override
  public void upsertSearchAttributes(SearchAttributes searchAttributes) {
    commands.addCommands(
        WorkflowCommand.newBuilder()
            .setUpsertWorkflowSearchAttributes(
                UpsertWorkflowSearchAttributes.newBuilder().setSearchAttributes(searchAttributes)));
    mutableState.upsertSearchAttributes(searchAttributes);
  }

  @Override
  public void upsertMemo(Memo memo) {
    commands.addCommands(
        WorkflowCommand.newBuilder()
            .setModifyWorkflowProperties(
                ModifyWorkflowProperties.newBuilder().setUpsertedMemo(memo)));
    mutableState.upsertMemo(memo);
  }

  @Override
  void cancelWorkflow() {
    commands.addCommands(
        WorkflowCommand.newBuilder()
            .setCancelWorkflowExecution(CancelWorkflowExecution.getDefaultInstance()));
  }

  @Override
  void failWorkflow(Failure failure) {
    commands.addCommands(
        WorkflowCommand.newBuilder()
            .setFailWorkflowExecution(FailWorkflowExecution.newBuilder().setFailure(failure)));
  }

  @Override
  void continueAsNewWorkflow(ContinueAsNewWorkflowExecutionCommandAttributes attributes) {
    throw new UnimplementedException("continueAsNew");
  }

  @Override
  void completeWorkflow(Optional<Payloads> result) {
    var command = CompleteWorkflowExecution.newBuilder();
    result.map(CoreReplayWorkflowContext::singlePayload).ifPresent(command::setResult);
    commands.addCommands(WorkflowCommand.newBuilder().setCompleteWorkflowExecution(command));
  }

  void respondToQuery(String id, Optional<Payloads> result, Failure failure) {
    var response = QueryResult.newBuilder().setQueryId(id);
    if (failure == null) {
      var success = QuerySuccess.newBuilder();
      result.map(CoreReplayWorkflowContext::singlePayload).ifPresent(success::setResponse);
      response.setSucceeded(success);
    } else {
      response.setFailed(failure);
    }
    commands.addCommands(WorkflowCommand.newBuilder().setRespondToQuery(response));
  }

  private static Payload singlePayload(Payloads payloads) {
    if (payloads.getPayloadsCount() > 1) {
      throw new IllegalArgumentException("Expected a single result payload");
    }
    return payloads.getPayloadsCount() == 0 ? null : payloads.getPayloads(0);
  }

  @Override
  public boolean getEnableLoggingInReplay() {
    return workerOptions.getEnableLoggingInReplay();
  }

  @Override
  public Scope getMetricsScope() {
    return replayAwareWorkflowMetricsScope;
  }

  @Nonnull
  @Override
  public WorkflowExecution getWorkflowExecution() {
    return basicWorkflowContext.getWorkflowExecution();
  }

  @Override
  public WorkflowExecution getParentWorkflowExecution() {
    return basicWorkflowContext.getParentWorkflowExecution();
  }

  @Override
  public WorkflowExecution getRootWorkflowExecution() {
    return basicWorkflowContext.getRootWorkflowExecution();
  }

  @Override
  public String getFirstExecutionRunId() {
    return basicWorkflowContext.getFirstExecutionRunId();
  }

  @Override
  public Optional<String> getContinuedExecutionRunId() {
    return basicWorkflowContext.getContinuedExecutionRunId();
  }

  @Override
  public String getOriginalExecutionRunId() {
    return basicWorkflowContext.getOriginalExecutionRunId();
  }

  @Override
  public WorkflowType getWorkflowType() {
    return basicWorkflowContext.getWorkflowType();
  }

  @Nonnull
  @Override
  public Duration getWorkflowTaskTimeout() {
    return basicWorkflowContext.getWorkflowTaskTimeout();
  }

  @Override
  public String getTaskQueue() {
    return basicWorkflowContext.getTaskQueue();
  }

  @Nullable
  @Override
  public RetryOptions getRetryOptions() {
    return basicWorkflowContext.getRetryOptions();
  }

  @Override
  public String getNamespace() {
    return basicWorkflowContext.getNamespace();
  }

  @Override
  public String getWorkflowId() {
    return basicWorkflowContext.getWorkflowExecution().getWorkflowId();
  }

  @Override
  public Duration getWorkflowRunTimeout() {
    return basicWorkflowContext.getWorkflowRunTimeout();
  }

  @Override
  public Duration getWorkflowExecutionTimeout() {
    return basicWorkflowContext.getWorkflowExecutionTimeout();
  }

  @Override
  public long getRunStartedTimestampMillis() {
    return basicWorkflowContext.getRunStartedTimestampMillis();
  }

  @Override
  public Payload getMemo(String key) {
    return mutableState.getMemo(key);
  }

  @Override
  @Nullable
  public SearchAttributes getSearchAttributes() {
    return mutableState.getSearchAttributes();
  }

  @Override
  public Priority getPriority() {
    return basicWorkflowContext.getPriority();
  }

  @Override
  public int getAttempt() {
    return basicWorkflowContext.getAttempt();
  }

  @Override
  public String getCronSchedule() {
    return basicWorkflowContext.getCronSchedule();
  }

  @Override
  @Nullable
  public Payloads getLastCompletionResult() {
    return basicWorkflowContext.getLastCompletionResult();
  }

  @Override
  @Nullable
  public Failure getPreviousRunFailure() {
    return basicWorkflowContext.getPreviousRunFailure();
  }

  @Override
  public Map<String, Payload> getHeader() {
    return basicWorkflowContext.getHeader();
  }

  @Override
  public boolean isCancelRequested() {
    return mutableState.isCancelRequested();
  }

  @Override
  public void setCancelRequested() {
    mutableState.setCancelRequested();
  }

  @Override
  public boolean isWorkflowMethodCompleted() {
    return mutableState.isWorkflowMethodCompleted();
  }

  @Override
  public void setWorkflowMethodCompleted() {
    this.mutableState.setWorkflowMethodCompleted();
  }

  @Override
  public ContinueAsNewWorkflowExecutionCommandAttributes getContinueAsNewOnCompletion() {
    return mutableState.getContinueAsNewOnCompletion();
  }

  @Override
  public Throwable getWorkflowTaskFailure() {
    return mutableState.getWorkflowTaskFailure();
  }

  @Override
  public void failWorkflowTask(Throwable failure) {
    mutableState.failWorkflowTask(failure);
  }

  @Override
  public ScheduleActivityTaskOutput scheduleActivityTask(
      ExecuteActivityParameters parameters, Functions.Proc2<Optional<Payloads>, Failure> callback) {
    throw new UnimplementedException("scheduleActivityTask");
  }

  @Override
  public Functions.Proc scheduleLocalActivityTask(
      ExecuteLocalActivityParameters parameters, LocalActivityCallback callback) {
    throw new UnimplementedException("scheduleLocalActivityTask");
  }

  @Override
  public Functions.Proc1<Exception> startChildWorkflow(
      StartChildWorkflowExecutionParameters parameters,
      Functions.Proc2<WorkflowExecution, Exception> startCallback,
      Functions.Proc2<Optional<Payloads>, Exception> completionCallback) {
    throw new UnimplementedException("startChildWorkflow");
  }

  @Override
  public Functions.Proc1<Exception> startNexusOperation(
      StartNexusOperationParameters parameters,
      Functions.Proc2<Optional<String>, Failure> startedCallback,
      Functions.Proc2<Optional<Payload>, Failure> completionCallback) {
    throw new UnimplementedException("startNexusOperation");
  }

  @Override
  public Functions.Proc1<Exception> signalExternalWorkflowExecution(
      SignalExternalWorkflowExecutionCommandAttributes.Builder attributes,
      Functions.Proc2<Void, Failure> callback) {
    throw new UnimplementedException("signalExternalWorkflowExecution");
  }

  @Override
  public void requestCancelExternalWorkflowExecution(
      WorkflowExecution execution,
      @Nullable String reason,
      Functions.Proc2<Void, RuntimeException> callback) {
    throw new UnimplementedException("requestCancelExternalWorkflowExecution");
  }

  @Override
  public void sideEffect(
      Func<Optional<Payloads>> func,
      UserMetadata metadata,
      Functions.Proc1<Optional<Payloads>> callback) {
    throw new UnimplementedException("sideEffect");
  }

  @Override
  public void mutableSideEffect(
      String id,
      UserMetadata metadata,
      Func1<Optional<Payloads>, Optional<Payloads>> func,
      Functions.Proc1<Optional<Payloads>> callback) {
    throw new UnimplementedException("mutableSideEffect");
  }

  @Override
  public Integer getVersion(
      String changeId,
      int minSupported,
      int maxSupported,
      Functions.Proc2<Integer, RuntimeException> callback) {
    throw new UnimplementedException("getVersion");
  }

  @Override
  public void continueAsNewOnCompletion(
      ContinueAsNewWorkflowExecutionCommandAttributes attributes) {
    throw new UnimplementedException("continueAsNewOnCompletion");
  }
}
