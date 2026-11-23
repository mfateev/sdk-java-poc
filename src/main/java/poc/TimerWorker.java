package poc;

import com.google.protobuf.Duration;
import coresdk.workflow_activation.WorkflowActivationOuterClass.WorkflowActivation;
import coresdk.workflow_commands.WorkflowCommands.CompleteWorkflowExecution;
import coresdk.workflow_commands.WorkflowCommands.StartTimer;
import coresdk.workflow_commands.WorkflowCommands.WorkflowCommand;
import coresdk.workflow_completion.WorkflowCompletion.Success;
import coresdk.workflow_completion.WorkflowCompletion.WorkflowActivationCompletion;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.channel.nio.NioEventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.socket.nio.NioSocketChannel;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/** Minimal workflow examples using the SDK Core protobuf boundary. */
public final class TimerWorker {
    public static void main(String[] args) throws Exception {
        String host = args.length > 0 ? args[0] : "127.0.0.1";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 7233;
        String queue = args.length > 2 ? args[2] : "java-poc";
        var eventLoop = new NioEventLoopGroup(1);
        var channel = NettyChannelBuilder.forAddress(host, port)
            .channelType(NioSocketChannel.class)
            .eventLoopGroup(eventLoop)
            .disableRetry()
            .usePlaintext()
            .build();
        if (queue.equals("--transport-only")) {
            try {
                CoreWorker.verifyTransport(channel);
                System.out.println("Core client received Temporal server capabilities over Java gRPC");
            } finally {
                channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
                eventLoop.shutdownGracefully(0, 2, TimeUnit.SECONDS).await(5, TimeUnit.SECONDS);
            }
            return;
        }
        try (var worker = new CoreWorker(channel, "default", queue, TimerWorker::activate)) {
            Thread shutdown = new Thread(() -> {
                worker.close();
                try {
                    worker.completion().get(10, TimeUnit.SECONDS);
                    System.out.println("Core worker shut down cleanly");
                } catch (Exception e) {
                    System.err.println("Worker shutdown: " + e);
                }
            }, "core-worker-shutdown");
            Runtime.getRuntime().addShutdownHook(shutdown);
            worker.completion().get();
        } finally {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            eventLoop.shutdownGracefully(0, 2, TimeUnit.SECONDS).await(5, TimeUnit.SECONDS);
        }
    }

    static byte[] activate(byte[] payload) throws IOException {
        WorkflowActivation activation = WorkflowActivation.parseFrom(payload);
        var success = Success.newBuilder();
        for (var job : activation.getJobsList()) {
            switch (job.getVariantCase()) {
                case INITIALIZE_WORKFLOW -> {
                    String type = job.getInitializeWorkflow().getWorkflowType();
                    switch (type) {
                        case "SingleTaskWorkflow" -> success.addCommands(completeWorkflow());
                        case "TimerWorkflow", "ShortTimerWorkflow" -> {
                            var delay = Duration.newBuilder();
                            if (type.equals("ShortTimerWorkflow")) {
                                delay.setNanos(50_000_000);
                            } else {
                                delay.setSeconds(2);
                            }
                            success.addCommands(WorkflowCommand.newBuilder()
                                .setStartTimer(StartTimer.newBuilder().setSeq(1).setStartToFireTimeout(delay)));
                        }
                        default -> throw new IOException("Unsupported workflow type " + type);
                    }
                    System.out.println("Java received InitializeWorkflow");
                }
                case FIRE_TIMER -> {
                    success.addCommands(completeWorkflow());
                    System.out.println("Java received FireTimer");
                }
                default -> {}
            }
        }
        return WorkflowActivationCompletion.newBuilder()
            .setRunId(activation.getRunId())
            .setSuccessful(success)
            .build()
            .toByteArray();
    }

    private static WorkflowCommand completeWorkflow() {
        return WorkflowCommand.newBuilder()
            .setCompleteWorkflowExecution(CompleteWorkflowExecution.getDefaultInstance())
            .build();
    }

    private TimerWorker() {}
}
