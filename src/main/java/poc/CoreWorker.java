package poc;

import io.grpc.Channel;
import io.temporal.common.VersioningBehavior;
import io.temporal.internal.worker.WorkerVersioningOptions;
import io.temporal.internal.worker.WorkerVersioningProtoUtils;
import org.rustlang.runtime.Pointer;
import temporal_java_bridge.temporal_java_bridge;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/** Runs an SDK Core workflow worker on a caller-owned Java gRPC channel. */
public final class CoreWorker implements AutoCloseable {
    @FunctionalInterface
    public interface ActivationHandler {
        /** Accepts a serialized WorkflowActivation and returns a serialized WorkflowActivationCompletion. */
        byte[] activate(byte[] activation) throws Exception;
    }

    @FunctionalInterface
    public interface LocalActivityHandler {
        /** Dispatches a serialized ActivityTask and returns a serialized completion asynchronously. */
        void dispatch(byte[] task, Consumer<byte[]> complete) throws Exception;
    }

    private static final String DEFAULT_BUILD_ID = "sdk-java-poc";
    private static final WorkerVersioningOptions DEFAULT_VERSIONING =
        new WorkerVersioningOptions(DEFAULT_BUILD_ID, false, null);
    private static final AtomicLong NEXT = new AtomicLong(1);
    private final long context = NEXT.getAndIncrement();
    private final CompletableFuture<Void> completion = new CompletableFuture<>();

    public CoreWorker(Channel channel, String namespace, String taskQueue, ActivationHandler handler) {
        this(channel, namespace, taskQueue, handler, null);
    }

    public CoreWorker(Channel channel, String namespace, String taskQueue, ActivationHandler handler,
                      LocalActivityHandler localActivities) {
        this(channel, namespace, taskQueue, handler, localActivities, DEFAULT_VERSIONING);
    }

    public CoreWorker(Channel channel, String namespace, String taskQueue, ActivationHandler handler,
                      LocalActivityHandler localActivities, WorkerVersioningOptions versioning) {
        if (namespace.isBlank() || taskQueue.isBlank()) {
            throw new IllegalArgumentException("namespace and task queue must be nonempty");
        }
        byte[] config = options(namespace, taskQueue, localActivities != null, versioning);
        JavaHost host = new JavaHost(Objects.requireNonNull(channel), Objects.requireNonNull(handler), localActivities);
        if (!temporal_java_bridge.register_worker(context)) {
            throw new IllegalStateException("Core worker ID already registered");
        }
        JavaHost.attach(context, host);
        Thread thread = new Thread(() -> {
            Throwable failure;
            try {
                temporal_java_bridge.run_worker(context, Pointer.array(config, 0, 1), config.length);
                failure = host.failure;
            } catch (Throwable error) {
                failure = error;
            } finally {
                temporal_java_bridge.forget_worker(context);
                JavaHost.detach(context);
            }
            if (failure == null) completion.complete(null);
            else completion.completeExceptionally(failure);
        }, "temporal-sdk-core-" + context);
        thread.start();
    }

    /** Exercises the Java transport by connecting a Core client and reading server capabilities. */
    public static void verifyTransport(Channel channel) {
        long context = NEXT.getAndIncrement();
        JavaHost host = new JavaHost(Objects.requireNonNull(channel), activation -> {
            throw new IllegalStateException("connection probe has no workflow handler");
        }, null);
        JavaHost.attach(context, host);
        try {
            temporal_java_bridge.probe_transport(context);
            if (host.failure != null) throw new IllegalStateException("Core transport probe failed", host.failure);
        } finally {
            JavaHost.detach(context);
        }
    }

    public CompletableFuture<Void> completion() {
        return completion.copy();
    }

    @Override public void close() {
        temporal_java_bridge.shutdown_worker(context);
    }

    private static byte[] options(String namespace, String queue, boolean localActivities,
                                  WorkerVersioningOptions versioning) {
        try {
            var bytes = new ByteArrayOutputStream();
            var output = new DataOutputStream(bytes);
            JavaHost.writeField(output, namespace.getBytes(StandardCharsets.UTF_8));
            JavaHost.writeField(output, queue.getBytes(StandardCharsets.UTF_8));
            output.writeBoolean(localActivities);
            var deployment = versioning.getWorkerDeploymentOptions();
            boolean hasDeployment = deployment != null && deployment.getVersion() != null;
            boolean legacyVersioning = !hasDeployment && versioning.isUsingVersioning();
            output.writeBoolean(legacyVersioning);
            String buildId = Objects.requireNonNullElse(versioning.getBuildId(), "");
            if (buildId.isEmpty() && !legacyVersioning) {
                buildId = DEFAULT_BUILD_ID;
            }
            JavaHost.writeField(output, buildId.getBytes(StandardCharsets.UTF_8));
            JavaHost.writeField(output, hasDeployment
                ? WorkerVersioningProtoUtils.deploymentOptionsToProto(deployment).toByteArray() : null);
            var defaultBehavior = hasDeployment && deployment.isUsingVersioning()
                ? Objects.requireNonNullElse(deployment.getDefaultVersioningBehavior(),
                    VersioningBehavior.UNSPECIFIED)
                : VersioningBehavior.UNSPECIFIED;
            output.writeInt(WorkerVersioningProtoUtils.behaviorToProto(defaultBehavior).getNumber());
            return bytes.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
