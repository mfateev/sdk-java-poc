package io.temporal.worker;

import io.temporal.internal.poc.Client;
import io.temporal.internal.poc.WorkflowDefinition;
import io.temporal.internal.poc.WorkflowRunner;
import io.temporal.serviceclient.WorkflowServiceStubs;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import poc.CoreWorker;

/** Registers workflow implementations for one task queue. */
public final class Worker {
    private static final System.Logger LOGGER = System.getLogger(Worker.class.getName());
    private final String taskQueue;
    private final WorkflowServiceStubs service;
    private final Map<String, WorkflowDefinition> definitions = new LinkedHashMap<>();
    private CoreWorker core;
    private CompletableFuture<Void> termination = CompletableFuture.completedFuture(null);
    private boolean started;

    Worker(String taskQueue, WorkflowServiceStubs service) {
        this.taskQueue = taskQueue;
        this.service = service;
    }

    public String getTaskQueue() {
        return taskQueue;
    }

    public synchronized void registerWorkflowImplementationTypes(Class<?>... implementationClasses) {
        if (started) {
            throw new IllegalStateException("Register workflows before starting the factory");
        }
        Map<String, WorkflowDefinition> added = new LinkedHashMap<>();
        for (Class<?> implementationClass : implementationClasses) {
            WorkflowDefinition definition = WorkflowDefinition.forImplementation(implementationClass);
            if (definitions.containsKey(definition.name()) || added.putIfAbsent(definition.name(), definition) != null) {
                throw new IllegalArgumentException("Workflow already registered: " + definition.name());
            }
        }
        definitions.putAll(added);
    }

    synchronized void start() {
        started = true;
        if (definitions.isEmpty()) {
            return;
        }
        var runner = new WorkflowRunner(definitions);
        core = new CoreWorker(service.getRawChannel(), Client.NAMESPACE, taskQueue, runner::activate);
        termination = core.completion().whenComplete((value, failure) -> {
            runner.close();
            if (failure != null) {
                LOGGER.log(System.Logger.Level.ERROR, "Core worker exited for " + taskQueue, failure);
            }
        });
    }

    synchronized CompletableFuture<Void> shutdown() {
        if (core != null) {
            core.close();
        }
        return termination;
    }
}
