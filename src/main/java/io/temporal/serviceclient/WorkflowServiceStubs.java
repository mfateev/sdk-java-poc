package io.temporal.serviceclient;

import io.grpc.ManagedChannel;
import io.temporal.internal.poc.ServiceStubs;
import java.util.concurrent.TimeUnit;

/** Owns the Java gRPC channel shared by workflow clients and workers. */
public interface WorkflowServiceStubs {
    static WorkflowServiceStubs newLocalServiceStubs() {
        return newServiceStubs(WorkflowServiceStubsOptions.getDefaultInstance());
    }

    static WorkflowServiceStubs newServiceStubs(WorkflowServiceStubsOptions options) {
        return new ServiceStubs(options);
    }

    ManagedChannel getRawChannel();

    void shutdown();

    void shutdownNow();

    boolean isShutdown();

    boolean isTerminated();

    boolean awaitTermination(long timeout, TimeUnit unit);
}
