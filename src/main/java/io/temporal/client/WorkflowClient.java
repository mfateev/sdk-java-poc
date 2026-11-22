package io.temporal.client;

import io.temporal.internal.poc.Client;
import io.temporal.serviceclient.WorkflowServiceStubs;

/** Creates typed workflow stubs. */
public interface WorkflowClient {
    static WorkflowClient newInstance(WorkflowServiceStubs service) {
        return new Client(service);
    }

    WorkflowServiceStubs getWorkflowServiceStubs();

    <T> T newWorkflowStub(Class<T> workflowInterface, WorkflowOptions options);
}
