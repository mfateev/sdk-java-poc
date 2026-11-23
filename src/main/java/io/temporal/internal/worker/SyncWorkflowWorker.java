package io.temporal.internal.worker;

import io.temporal.client.WorkflowClient;
import io.temporal.common.converter.EncodedValues;
import io.temporal.internal.common.WorkflowExecutionHistory;
import io.temporal.internal.replay.CoreWorkflowActivationHandler;
import io.temporal.internal.sync.POJOWorkflowImplementationFactory;
import io.temporal.internal.sync.WorkflowThreadExecutor;
import io.temporal.worker.WorkflowImplementationOptions;
import io.temporal.worker.WorkflowTaskDispatchHandle;
import io.temporal.worker.tuning.LocalActivitySlotInfo;
import io.temporal.worker.tuning.SlotSupplier;
import io.temporal.worker.tuning.WorkflowSlotInfo;
import io.temporal.workflow.Functions.Func;
import io.temporal.workflow.Functions.Func1;
import java.lang.reflect.Type;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import poc.CoreWorker;

/** Assembles the existing Java workflow runtime and a Core worker for one task queue. */
public final class SyncWorkflowWorker implements SuspendableWorker {
  private static final Logger log = LoggerFactory.getLogger(SyncWorkflowWorker.class);
  private final WorkflowClient client;
  private final String namespace;
  private final String taskQueue;
  private final SingleWorkerOptions options;
  private final POJOWorkflowImplementationFactory factory;
  private CoreWorker core;
  private CompletableFuture<Void> termination = CompletableFuture.completedFuture(null);
  private boolean shutdown;

  // Keep the SDK Java Worker's assembly signature while Core owns workflow task scheduling.
  public SyncWorkflowWorker(
      WorkflowClient client,
      String namespace,
      String taskQueue,
      SingleWorkerOptions options,
      SingleWorkerOptions localActivityOptions,
      WorkflowRunLockManager runLocks,
      WorkflowExecutorCache cache,
      String stickyTaskQueueName,
      WorkflowThreadExecutor workflowThreadExecutor,
      EagerActivityDispatcher eagerActivityDispatcher,
      SlotSupplier<WorkflowSlotInfo> slotSupplier,
      SlotSupplier<LocalActivitySlotInfo> laSlotSupplier) {
    this.client = client;
    this.namespace = namespace;
    this.taskQueue = taskQueue;
    this.options = options;
    this.factory =
        new POJOWorkflowImplementationFactory(
            options, workflowThreadExecutor, options.getWorkerInterceptors(), null, namespace);
  }

  public void registerWorkflowImplementationTypes(
      WorkflowImplementationOptions options, Class<?>[] implementationTypes) {
    factory.registerWorkflowImplementationTypes(options, implementationTypes);
  }

  public <R> void registerWorkflowImplementationFactory(
      WorkflowImplementationOptions options, Class<R> type, Func<R> implementation) {
    factory.addWorkflowImplementationFactory(options, type, ignored -> implementation.apply());
  }

  public <R> void registerWorkflowImplementationFactory(
      WorkflowImplementationOptions options,
      Class<R> type,
      Func1<EncodedValues, R> implementation) {
    factory.addWorkflowImplementationFactory(options, type, implementation);
  }

  public void registerLocalActivityImplementations(Object... implementations) {
    throw new UnsupportedOperationException("Activities are outside the Core worker POC");
  }

  @Override
  public synchronized boolean start() {
    if (core != null || shutdown || !factory.isAnyTypeSupported()) {
      return false;
    }
    var handler = new CoreWorkflowActivationHandler(factory, options, namespace, taskQueue);
    core =
        new CoreWorker(
            client.getWorkflowServiceStubs().getRawChannel(),
            namespace,
            taskQueue,
            handler::activate);
    termination =
        core.completion()
            .whenComplete(
                (ignored, failure) -> {
                  handler.close();
                  if (failure != null) {
                    log.error("Core workflow worker exited for task queue {}", taskQueue, failure);
                  }
                });
    return true;
  }

  @Override
  public synchronized CompletableFuture<Void> shutdown(
      ShutdownManager manager, boolean interruptTasks) {
    if (!shutdown) {
      shutdown = true;
      if (core != null) {
        core.close();
      }
    }
    return termination;
  }

  @Override
  public void awaitTermination(long timeout, TimeUnit unit) {
    CompletableFuture<Void> completion;
    synchronized (this) {
      completion = termination;
    }
    try {
      completion.get(timeout, unit);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (ExecutionException | TimeoutException ignored) {
      // A failed worker is terminated. A timed-out wait leaves termination outstanding.
    }
  }

  @Override
  public synchronized boolean isShutdown() {
    return shutdown;
  }

  @Override
  public synchronized boolean isTerminated() {
    return shutdown && termination.isDone();
  }

  @Override
  public synchronized WorkerLifecycleState getLifecycleState() {
    if (shutdown) {
      return isTerminated() ? WorkerLifecycleState.TERMINATED : WorkerLifecycleState.SHUTDOWN;
    }
    return core == null ? WorkerLifecycleState.NOT_STARTED : WorkerLifecycleState.ACTIVE;
  }

  @Override
  public void suspendPolling() {
    throw new UnsupportedOperationException("Suspending polling is outside the Core worker POC");
  }

  @Override
  public void resumePolling() {
    throw new UnsupportedOperationException("Resuming polling is outside the Core worker POC");
  }

  @Override
  public boolean isSuspended() {
    return false;
  }

  public <R> R queryWorkflowExecution(
      WorkflowExecutionHistory history,
      String queryType,
      Class<R> resultClass,
      Type resultType,
      Object[] args) {
    throw new UnsupportedOperationException(
        "Offline history replay is outside the Core worker POC");
  }

  public WorkflowTaskDispatchHandle reserveWorkflowExecutor() {
    return null; // The gRPC client falls back to a server-polled workflow task.
  }
}
