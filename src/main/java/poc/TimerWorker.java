package poc;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.WireFormat;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.channel.nio.NioEventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.socket.nio.NioSocketChannel;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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

    // The example only needs a few fields of the pinned activation/completion protos.
    static byte[] activate(byte[] payload) throws IOException {
        String run = "";
        List<byte[]> commands = new ArrayList<>();
        var activation = CodedInputStream.newInstance(payload);
        for (int tag; (tag = activation.readTag()) != 0; ) {
            switch (WireFormat.getTagFieldNumber(tag)) {
                case 1 -> run = activation.readStringRequireUtf8();
                case 5 -> addCommand(activation.readByteArray(), commands);
                default -> activation.skipField(tag);
            }
        }
        String runId = run;
        return encode(out -> {
            out.writeString(1, runId);
            out.writeByteArray(2, encode(success -> {
                for (byte[] command : commands) success.writeByteArray(1, command);
            }));
        });
    }

    private static void addCommand(byte[] payload, List<byte[]> commands) throws IOException {
        var job = CodedInputStream.newInstance(payload);
        for (int tag; (tag = job.readTag()) != 0; ) {
            switch (WireFormat.getTagFieldNumber(tag)) {
                case 1 -> {
                    var initialize = CodedInputStream.newInstance(job.readByteArray());
                    String workflowType = "";
                    for (int field; (field = initialize.readTag()) != 0; ) {
                        if (WireFormat.getTagFieldNumber(field) == 1) {
                            workflowType = initialize.readStringRequireUtf8();
                        } else {
                            initialize.skipField(field);
                        }
                    }
                    if (workflowType.equals("SingleTaskWorkflow")) {
                        commands.add(encode(out -> out.writeByteArray(6, new byte[0])));
                    } else if (workflowType.equals("TimerWorkflow") || workflowType.equals("ShortTimerWorkflow")) {
                        boolean shortTimer = workflowType.equals("ShortTimerWorkflow");
                        commands.add(encode(out -> out.writeByteArray(1, encode(timer -> {
                            timer.writeUInt32(1, 1);
                            timer.writeByteArray(2, encode(duration -> {
                                if (shortTimer) duration.writeInt32(2, 50_000_000);
                                else duration.writeInt64(1, 2);
                            }));
                        }))));
                    } else {
                        throw new IOException("unsupported workflow type " + workflowType);
                    }
                    System.out.println("Java received InitializeWorkflow");
                }
                case 2 -> {
                    job.skipField(tag);
                    commands.add(encode(out -> out.writeByteArray(6, new byte[0])));
                    System.out.println("Java received FireTimer");
                }
                default -> job.skipField(tag);
            }
        }
    }

    @FunctionalInterface
    private interface Encoder {
        void write(CodedOutputStream out) throws IOException;
    }

    private static byte[] encode(Encoder encoder) throws IOException {
        var bytes = new ByteArrayOutputStream();
        var out = CodedOutputStream.newInstance(bytes);
        encoder.write(out);
        out.flush();
        return bytes.toByteArray();
    }

    private TimerWorker() {}
}
