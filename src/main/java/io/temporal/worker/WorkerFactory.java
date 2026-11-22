package io.temporal.worker;

import io.temporal.client.WorkflowClient;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Creates and manages a set of workers sharing a workflow client. */
public final class WorkerFactory {
    private final WorkflowClient client;
    private final Map<String, Worker> workers = new LinkedHashMap<>();
    private final CompletableFuture<Void> terminated = new CompletableFuture<>();
    private boolean started;
    private boolean shutdown;

    private WorkerFactory(WorkflowClient client) {
        this.client = Objects.requireNonNull(client);
    }

    public static WorkerFactory newInstance(WorkflowClient workflowClient) {
        return new WorkerFactory(workflowClient);
    }

    public synchronized Worker newWorker(String taskQueue) {
        if (started || shutdown) {
            throw new IllegalStateException("Create workers before starting the factory");
        }
        if (taskQueue == null || taskQueue.isBlank()) {
            throw new IllegalArgumentException("Task queue is required");
        }
        return workers.computeIfAbsent(taskQueue, queue -> new Worker(queue, client.getWorkflowServiceStubs()));
    }

    public synchronized void start() {
        if (shutdown) {
            throw new IllegalStateException("Factory is shut down");
        }
        if (started) {
            return;
        }
        started = true;
        try {
            workers.values().forEach(Worker::start);
        } catch (RuntimeException | Error error) {
            shutdown();
            throw error;
        }
    }

    public synchronized void shutdown() {
        if (shutdown) {
            return;
        }
        shutdown = true;
        CompletableFuture.allOf(workers.values().stream().map(Worker::shutdown)
            .toArray(CompletableFuture<?>[]::new))
            .whenComplete((value, failure) -> terminated.complete(null));
    }

    public boolean isTerminated() {
        return terminated.isDone();
    }

    public void awaitTermination(long timeout, TimeUnit unit) {
        try {
            terminated.get(timeout, unit);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (TimeoutException ignored) {
            // Awaiting termination is bounded by the caller's timeout.
        } catch (ExecutionException e) {
            throw new IllegalStateException("Worker factory termination failed", e.getCause());
        }
    }
}
