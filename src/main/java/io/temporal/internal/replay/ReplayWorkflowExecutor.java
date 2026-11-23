package io.temporal.internal.replay;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.Timestamps;
import io.temporal.api.command.v1.ContinueAsNewWorkflowExecutionCommandAttributes;
import io.temporal.api.common.v1.Header;
import io.temporal.api.common.v1.Payloads;
import io.temporal.api.failure.v1.Failure;
import io.temporal.api.history.v1.HistoryEvent;
import io.temporal.api.history.v1.WorkflowExecutionSignaledEventAttributes;
import io.temporal.api.protocol.v1.Message;
import io.temporal.api.query.v1.WorkflowQuery;
import io.temporal.api.update.v1.Input;
import io.temporal.api.update.v1.Request;
import io.temporal.failure.CanceledFailure;
import io.temporal.internal.common.ProtobufTimeUtils;
import io.temporal.internal.common.UpdateMessage;
import io.temporal.internal.statemachines.WorkflowStateMachines;
import io.temporal.internal.sync.SignalHandlerInfo;
import io.temporal.internal.sync.UpdateHandlerInfo;
import io.temporal.internal.worker.WorkflowExecutionException;
import io.temporal.worker.NonDeterministicException;
import io.temporal.workflow.HandlerUnfinishedPolicy;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

final class ReplayWorkflowExecutor {
  @VisibleForTesting
  public static final String unfinishedUpdateHandlesWarnMessage =
      "[TMPRL1102] Workflow finished while update handlers are still running. This may "
          + "have interrupted work that the update handler was doing, and the client "
          + "that sent the update will receive a 'workflow execution already completed' "
          + "Exception instead of the update result. You can wait for all update and "
          + "signal handlers to complete by using `await workflow.Await(() -> workflow.isEveryHandlerFinished())`. "
          + "Alternatively, if both you and the clients sending the update are okay with "
          + "interrupting running handlers when the workflow finishes, and causing "
          + "clients to receive errors, then you can disable this warning via the update "
          + "handler annotations: `@UpdateMethod(unfinishedPolicy = HandlerUnfinishedPolicy.ABANDON)`.";

  @VisibleForTesting
  public static final String unfinishedSignalHandlesWarnMessage =
      "[TMPRL1102] Workflow finished while signal handlers are still running. This may "
          + "have interrupted work that the signal handler was doing. You can wait for all update and "
          + "signal handlers to complete by using `await workflow.Await(() -> workflow.isEveryHandlerFinished())`. "
          + "Alternatively, if both you and the clients sending the signal are okay with "
          + "interrupting running handlers when the workflow finishes you can disable this warning via the signal "
          + "handler annotations: `@SignalMethod(unfinishedPolicy = HandlerUnfinishedPolicy.ABANDON)`.";

  private static final Logger log = LoggerFactory.getLogger(ReplayWorkflowExecutor.class);

  private final ReplayWorkflow workflow;

  interface CompletionSink {
    void cancelWorkflow();

    void failWorkflow(Failure failure);

    void continueAsNewWorkflow(ContinueAsNewWorkflowExecutionCommandAttributes attributes);

    void completeWorkflow(Optional<Payloads> result);

    default void recordCompletionLatency(com.uber.m3.util.Duration latency) {}
  }

  private final CompletionSink completion;
  private final ReplayWorkflowContext context;

  public ReplayWorkflowExecutor(
      ReplayWorkflow workflow,
      WorkflowStateMachines workflowStateMachines,
      ReplayWorkflowContextImpl context) {
    this(
        workflow,
        context,
        new CompletionSink() {
          @Override
          public void cancelWorkflow() {
            workflowStateMachines.cancelWorkflow();
          }

          @Override
          public void failWorkflow(Failure failure) {
            workflowStateMachines.failWorkflow(failure);
          }

          @Override
          public void continueAsNewWorkflow(
              ContinueAsNewWorkflowExecutionCommandAttributes attributes) {
            workflowStateMachines.continueAsNewWorkflow(attributes);
          }

          @Override
          public void completeWorkflow(Optional<Payloads> result) {
            workflowStateMachines.completeWorkflow(result);
          }

          @Override
          public void recordCompletionLatency(com.uber.m3.util.Duration latency) {
            workflowStateMachines.setPostCompletionEndToEndLatency(latency);
          }
        });
  }

  ReplayWorkflowExecutor(
      ReplayWorkflow workflow, ReplayWorkflowContext context, CompletionSink completion) {
    this.workflow = workflow;
    this.context = context;
    this.completion = completion;
  }

  public void eventLoop() {
    boolean completed = context.isWorkflowMethodCompleted();
    if (completed) {
      return;
    }
    WorkflowExecutionException failure = null;

    try {
      completed = workflow.eventLoop();
    } catch (WorkflowExecutionException e) {
      failure = e;
      completed = true;
    } catch (CanceledFailure e) {
      if (!context.isCancelRequested()) {
        failure =
            new WorkflowExecutionException(
                workflow.getWorkflowContext().mapWorkflowExceptionToFailure(e));
      }
      completed = true;
    }
    if (completed) {
      context.setWorkflowMethodCompleted();
      completeWorkflow(failure);
    }
  }

  private void completeWorkflow(@Nullable WorkflowExecutionException failure) {
    // If the workflow is failed we do not log any warnings about unfinished handlers.
    if (log.isWarnEnabled() && (failure == null || context.isCancelRequested())) {
      Map<Long, SignalHandlerInfo> runningSignalHandlers =
          workflow.getWorkflowContext().getRunningSignalHandlers();
      List<SignalHandlerInfo> unfinishedSignalHandlers =
          runningSignalHandlers.values().stream()
              .filter(a -> a.getPolicy() == HandlerUnfinishedPolicy.WARN_AND_ABANDON)
              .collect(Collectors.toList());
      if (!unfinishedSignalHandlers.isEmpty()) {
        MDC.put("Signals", unfinishedSignalHandlers.toString());
        log.warn(unfinishedSignalHandlesWarnMessage);
        MDC.remove("Signals");
      }

      Map<String, UpdateHandlerInfo> runningUpdateHandlers =
          workflow.getWorkflowContext().getRunningUpdateHandlers();
      List<UpdateHandlerInfo> unfinishedUpdateHandlers =
          runningUpdateHandlers.values().stream()
              .filter(a -> a.getPolicy() == HandlerUnfinishedPolicy.WARN_AND_ABANDON)
              .collect(Collectors.toList());
      if (!unfinishedUpdateHandlers.isEmpty()) {
        MDC.put("Updates", unfinishedUpdateHandlers.toString());
        log.warn(unfinishedUpdateHandlesWarnMessage);
        MDC.remove("Updates");
      }
    }

    if (context.isCancelRequested()) {
      completion.cancelWorkflow();
    } else if (failure != null) {
      completion.failWorkflow(failure.getFailure());
    } else {
      ContinueAsNewWorkflowExecutionCommandAttributes attributes =
          context.getContinueAsNewOnCompletion();
      if (attributes != null) {
        // TODO Refactoring idea
        //  Instead of carrying attributes over like this, SyncWorkflowContext should call
        //  completion.continueAsNewWorkflow directly.
        //  It's safe to do and be sure that ContinueAsNew will be the last command because
        //  WorkflowThread.exit() that it called and it's underlying implementation
        //  DeterministicRunner#exit()
        //  guarantee that no other workflow threads will get unblocked,
        //  so no new commands are generated after the call.
        //  This way attributes will need to be carried over in the mutable state and the flow
        //  generally will be aligned with the flow of other commands.
        completion.continueAsNewWorkflow(attributes);
      } else {
        Optional<Payloads> workflowOutput = workflow.getOutput();
        completion.completeWorkflow(workflowOutput);
      }
    }

    com.uber.m3.util.Duration d =
        ProtobufTimeUtils.toM3Duration(
            Timestamps.fromMillis(System.currentTimeMillis()),
            Timestamps.fromMillis(context.getRunStartedTimestampMillis()));
    completion.recordCompletionLatency(d);
  }

  public void handleWorkflowExecutionCancelRequested(HistoryEvent event) {
    handleCancel(event.getWorkflowExecutionCancelRequestedEventAttributes().getCause());
  }

  void handleCancel(String reason) {
    context.setCancelRequested();
    workflow.cancel(reason);
  }

  public void handleWorkflowExecutionSignaled(HistoryEvent event) {
    WorkflowExecutionSignaledEventAttributes attributes =
        event.getWorkflowExecutionSignaledEventAttributes();
    Optional<Payloads> input =
        attributes.hasInput() ? Optional.of(attributes.getInput()) : Optional.empty();
    handleSignal(attributes.getSignalName(), input, event.getEventId(), attributes.getHeader());
  }

  void handleSignal(String name, Optional<Payloads> input, long eventId, Header header) {
    if (context.isWorkflowMethodCompleted()) {
      throw new NonDeterministicException(
          "Signal received after workflow is completed. Typically this is caused by a"
              + " nondeterministic code change in a workflow or a change is what payloads data"
              + " converters can handle");
    }
    workflow.handleSignal(name, input, eventId, header);
  }

  public void handleWorkflowExecutionUpdated(UpdateMessage updateMessage) {
    if (context.isWorkflowMethodCompleted()) {
      throw new NonDeterministicException("Update received after workflow is completed.");
    }
    try {
      Message protocolMessage = updateMessage.getMessage();
      Request update = protocolMessage.getBody().unpack(Request.class);
      Input input = update.getInput();
      Optional<Payloads> args = Optional.of(input.getArgs());
      this.workflow.handleUpdate(
          input.getName(),
          update.getMeta().getUpdateId(),
          args,
          protocolMessage.getEventId(),
          input.getHeader(),
          updateMessage.getCallbacks());
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalStateException("Message is not an update.");
    }
  }

  public Optional<Payloads> query(WorkflowQuery query) {
    return workflow.query(query);
  }

  public void close() {
    workflow.close();
  }

  public void start(HistoryEvent startWorkflowEvent) {
    workflow.start(startWorkflowEvent, context);
  }
}
