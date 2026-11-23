package io.temporal.internal.worker;

import com.google.protobuf.ByteString;
import coresdk.CoreInterface.ActivityTaskCompletion;
import coresdk.activity_result.ActivityResult;
import coresdk.activity_task.ActivityTaskOuterClass;
import io.temporal.api.common.v1.ActivityType;
import io.temporal.api.common.v1.Header;
import io.temporal.api.common.v1.Payloads;
import io.temporal.api.common.v1.WorkflowType;
import io.temporal.api.failure.v1.CanceledFailureInfo;
import io.temporal.api.failure.v1.Failure;
import io.temporal.api.workflowservice.v1.PollActivityTaskQueueResponse;
import io.temporal.internal.activity.ActivityTaskHandlerImpl;
import io.temporal.worker.tuning.SlotPermit;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/** Runs Core-dispatched local activities through SDK Java's activity handler. */
final class CoreLocalActivityHandler implements AutoCloseable {
  private final ActivityTaskHandlerImpl handler;
  private final SingleWorkerOptions options;
  private final ConcurrentHashMap<ByteString, Execution> executions = new ConcurrentHashMap<>();

  CoreLocalActivityHandler(ActivityTaskHandlerImpl handler, SingleWorkerOptions options) {
    this.handler = handler;
    this.options = options;
  }

  void dispatch(byte[] bytes, Consumer<byte[]> complete) throws IOException {
    var task = ActivityTaskOuterClass.ActivityTask.parseFrom(bytes);
    ByteString token = task.getTaskToken();
    switch (task.getVariantCase()) {
      case START -> {
        var execution = new Execution(task, complete);
        if (executions.putIfAbsent(token, execution) != null) {
          throw new IllegalStateException("Core dispatched a duplicate local activity task");
        }
        execution.thread.start();
      }
      case CANCEL -> {
        Execution execution = executions.get(token);
        if (execution != null) {
          execution.cancel();
        }
      }
      case VARIANT_NOT_SET ->
          throw new IllegalArgumentException("Core activity task has no variant");
    }
  }

  @Override
  public void close() {
    executions.values().forEach(Execution::cancel);
  }

  private ActivityResult.ActivityExecutionResult execute(ActivityTaskOuterClass.ActivityTask task) {
    var start = task.getStart();
    var response =
        PollActivityTaskQueueResponse.newBuilder()
            .setTaskToken(task.getTaskToken())
            .setWorkflowNamespace(start.getWorkflowNamespace())
            .setWorkflowExecution(start.getWorkflowExecution())
            .setWorkflowType(WorkflowType.newBuilder().setName(start.getWorkflowType()))
            .setActivityId(start.getActivityId())
            .setActivityType(ActivityType.newBuilder().setName(start.getActivityType()))
            .setHeader(Header.newBuilder().putAllFields(start.getHeaderFieldsMap()))
            .setInput(Payloads.newBuilder().addAllPayloads(start.getInputList()))
            .setHeartbeatDetails(
                Payloads.newBuilder().addAllPayloads(start.getHeartbeatDetailsList()))
            .setScheduledTime(start.getScheduledTime())
            .setCurrentAttemptScheduledTime(start.getCurrentAttemptScheduledTime())
            .setStartedTime(start.getStartedTime())
            .setAttempt(start.getAttempt());
    if (start.hasScheduleToCloseTimeout())
      response.setScheduleToCloseTimeout(start.getScheduleToCloseTimeout());
    if (start.hasStartToCloseTimeout())
      response.setStartToCloseTimeout(start.getStartToCloseTimeout());
    if (start.hasRetryPolicy()) response.setRetryPolicy(start.getRetryPolicy());

    ActivityTaskHandler.Result handled =
        handler.handle(
            new ActivityTask(response, new SlotPermit(), () -> {}),
            options.getMetricsScope(),
            true);
    var result = ActivityResult.ActivityExecutionResult.newBuilder();
    if (handled.getTaskCompleted() != null) {
      var success = ActivityResult.Success.newBuilder();
      var payloads = handled.getTaskCompleted().getResult();
      if (payloads.getPayloadsCount() > 1) {
        throw new IllegalArgumentException("Expected a single local activity result payload");
      }
      if (payloads.getPayloadsCount() == 1) {
        success.setResult(payloads.getPayloads(0));
      }
      return result.setCompleted(success).build();
    }
    if (handled.getTaskFailed() != null) {
      var failure = handled.getTaskFailed().getTaskFailedRequest();
      return result
          .setFailed(ActivityResult.Failure.newBuilder().setFailure(failure.getFailure()))
          .build();
    }
    if (handled.getTaskCanceled() != null) {
      return cancelled(
          CanceledFailureInfo.newBuilder()
              .setDetails(handled.getTaskCanceled().getDetails())
              .build());
    }
    throw new IllegalStateException(
        "A local activity must return a result in the worker that runs it");
  }

  private static ActivityResult.ActivityExecutionResult cancelled(CanceledFailureInfo info) {
    return ActivityResult.ActivityExecutionResult.newBuilder()
        .setCancelled(
            ActivityResult.Cancellation.newBuilder()
                .setFailure(
                    Failure.newBuilder()
                        .setMessage("Local activity canceled")
                        .setCanceledFailureInfo(info)))
        .build();
  }

  private final class Execution {
    private final Thread thread;
    private volatile boolean canceled;

    Execution(ActivityTaskOuterClass.ActivityTask task, Consumer<byte[]> complete) {
      thread =
          Thread.ofVirtual()
              .name("temporal-core-local-activity")
              .unstarted(
                  () -> {
                    var token = task.getTaskToken();
                    ActivityResult.ActivityExecutionResult result;
                    try {
                      result =
                          canceled
                              ? cancelled(CanceledFailureInfo.getDefaultInstance())
                              : execute(task);
                    } catch (Exception | Error failure) {
                      result =
                          ActivityResult.ActivityExecutionResult.newBuilder()
                              .setFailed(
                                  ActivityResult.Failure.newBuilder()
                                      .setFailure(
                                          options.getDataConverter().exceptionToFailure(failure)))
                              .build();
                    } finally {
                      executions.remove(token, this);
                    }
                    if (canceled && !result.hasCancelled()) {
                      result = cancelled(CanceledFailureInfo.getDefaultInstance());
                    }
                    complete.accept(
                        ActivityTaskCompletion.newBuilder()
                            .setTaskToken(token)
                            .setResult(result)
                            .build()
                            .toByteArray());
                  });
    }

    void cancel() {
      canceled = true;
      thread.interrupt();
    }
  }
}
