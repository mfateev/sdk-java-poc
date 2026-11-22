package io.temporal.internal.poc;

import static io.temporal.internal.poc.Proto.API;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.stub.ClientCalls;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.serviceclient.WorkflowServiceStubs;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.lang.reflect.Proxy;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public final class Client implements WorkflowClient {
    public static final String NAMESPACE = "default";
    private static final String SERVICE = API + "workflowservice.v1.";
    private static final MethodDescriptor<DynamicMessage, DynamicMessage> START = rpc("StartWorkflowExecution");
    private static final MethodDescriptor<DynamicMessage, DynamicMessage> HISTORY = rpc("GetWorkflowExecutionHistory");
    private final WorkflowServiceStubs service;
    private final String identity = "sdk-java-poc-" + UUID.randomUUID();

    public Client(WorkflowServiceStubs service) {
        this.service = Objects.requireNonNull(service);
    }

    @Override public WorkflowServiceStubs getWorkflowServiceStubs() {
        return service;
    }

    @Override public <T> T newWorkflowStub(Class<T> workflowInterface, WorkflowOptions options) {
        WorkflowDefinition definition = WorkflowDefinition.forInterface(workflowInterface);
        Objects.requireNonNull(options);
        if (options.getTaskQueue() == null || options.getTaskQueue().isBlank()) {
            throw new IllegalArgumentException("A workflow task queue is required");
        }
        String workflowId = options.getWorkflowId() == null
            ? UUID.randomUUID().toString() : options.getWorkflowId();
        if (workflowId.isBlank()) {
            throw new IllegalArgumentException("Workflow ID must be nonempty");
        }
        return workflowInterface.cast(Proxy.newProxyInstance(
            workflowInterface.getClassLoader(),
            new Class<?>[] {workflowInterface},
            (proxy, method, args) -> {
                if (method.equals(definition.method())) {
                    return execute(definition, workflowId, options.getTaskQueue(), args == null ? new Object[0] : args);
                }
                if (method.getDeclaringClass() == Object.class) {
                    return switch (method.getName()) {
                        case "equals" -> proxy == args[0];
                        case "hashCode" -> System.identityHashCode(proxy);
                        case "toString" -> "Workflow stub for " + definition.name() + " (" + workflowId + ")";
                        default -> throw new UnsupportedOperationException(method.toString());
                    };
                }
                throw new UnsupportedOperationException("Only the workflow method is supported: " + method);
            }));
    }

    private String execute(WorkflowDefinition definition, String workflowId, String taskQueue, Object[] args) {
        var request = Proto.builder(SERVICE + "StartWorkflowExecutionRequest");
        Proto.set(request, "namespace", NAMESPACE);
        Proto.set(request, "workflow_id", workflowId);
        Proto.set(request, "workflow_type", Proto.message(API + "common.v1.WorkflowType", "name", definition.name()));
        Proto.set(request, "task_queue", Proto.message(API + "taskqueue.v1.TaskQueue", "name", taskQueue));
        Proto.set(request, "identity", identity);
        Proto.set(request, "request_id", UUID.randomUUID().toString());
        if (args.length != 0) {
            Proto.set(request, "input", Payloads.encodeArguments(args));
        }
        DynamicMessage started = call(START, request.build(), 10);

        var execution = Proto.builder(API + "common.v1.WorkflowExecution");
        Proto.set(execution, "workflow_id", workflowId);
        Proto.set(execution, "run_id", Proto.get(started, "run_id"));

        var history = Proto.builder(SERVICE + "GetWorkflowExecutionHistoryRequest");
        Proto.set(history, "namespace", NAMESPACE);
        Proto.set(history, "execution", execution.build());
        Proto.set(history, "wait_new_event", true);
        Proto.setEnum(history, "history_event_filter_type", "HISTORY_EVENT_FILTER_TYPE_CLOSE_EVENT");
        while (true) {
            DynamicMessage response = call(HISTORY, history.build(), 70);
            DynamicMessage page = (DynamicMessage) Proto.get(response, "history");
            for (DynamicMessage event : Proto.messages(page, "events")) {
                if (!Proto.has(event, "workflow_execution_completed_event_attributes")) {
                    throw new IllegalStateException("Workflow " + workflowId + " closed with " + Proto.get(event, "event_type"));
                }
                DynamicMessage completed = (DynamicMessage) Proto.get(event, "workflow_execution_completed_event_attributes");
                if (definition.method().getReturnType() == void.class || !Proto.has(completed, "result")) {
                    return null;
                }
                DynamicMessage result = (DynamicMessage) Proto.get(completed, "result");
                var payloads = Proto.messages(result, "payloads");
                try {
                    return payloads.isEmpty() ? null : Payloads.decode(payloads.getFirst());
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
            Proto.set(history, "next_page_token", (ByteString) Proto.get(response, "next_page_token"));
        }
    }

    private DynamicMessage call(
            MethodDescriptor<DynamicMessage, DynamicMessage> method, DynamicMessage request, long seconds) {
        return ClientCalls.blockingUnaryCall(service.getRawChannel(), method,
            CallOptions.DEFAULT.withDeadlineAfter(seconds, TimeUnit.SECONDS), request);
    }

    private static MethodDescriptor<DynamicMessage, DynamicMessage> rpc(String name) {
        return MethodDescriptor.<DynamicMessage, DynamicMessage>newBuilder()
            .setType(MethodDescriptor.MethodType.UNARY)
            .setFullMethodName(MethodDescriptor.generateFullMethodName(SERVICE + "WorkflowService", name))
            .setRequestMarshaller(marshaller(Proto.type(SERVICE + name + "Request")))
            .setResponseMarshaller(marshaller(Proto.type(SERVICE + name + "Response")))
            .build();
    }

    private static MethodDescriptor.Marshaller<DynamicMessage> marshaller(Descriptor type) {
        return new MethodDescriptor.Marshaller<>() {
            @Override public InputStream stream(DynamicMessage value) {
                return value.toByteString().newInput();
            }

            @Override public DynamicMessage parse(InputStream stream) {
                try {
                    return DynamicMessage.parseFrom(type, stream);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        };
    }
}
