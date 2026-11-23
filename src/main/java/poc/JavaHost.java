package poc;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import org.rustlang.runtime.Pointer;
import temporal_java_bridge.temporal_java_bridge;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public final class JavaHost {
    private static final Set<String> RESERVED_HEADERS = Set.of(
        "content-type", "te", "user-agent", "grpc-encoding", "grpc-accept-encoding",
        "grpc-status", "grpc-message", "grpc-status-details-bin", "grpc-timeout");
    private static final Metadata.Key<byte[]> STATUS_DETAILS =
        Metadata.Key.of("grpc-status-details-bin", Metadata.BINARY_BYTE_MARSHALLER);
    private static final ConcurrentHashMap<Long, JavaHost> HOSTS = new ConcurrentHashMap<>();
    private static final MethodDescriptor.Marshaller<byte[]> BYTES =
        new MethodDescriptor.Marshaller<>() {
            @Override public InputStream stream(byte[] value) { return new ByteArrayInputStream(value); }

            @Override public byte[] parse(InputStream stream) {
                try {
                    return stream.readAllBytes();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        };

    private final Channel channel;
    private final CoreWorker.ActivationHandler handler;
    private final CoreWorker.LocalActivityHandler localActivities;
    private final ConcurrentHashMap<Long, ClientCall<byte[], byte[]>> calls = new ConcurrentHashMap<>();
    private byte[] completion;
    volatile Throwable failure;

    JavaHost(Channel channel, CoreWorker.ActivationHandler handler, CoreWorker.LocalActivityHandler localActivities) {
        this.channel = channel;
        this.handler = handler;
        this.localActivities = localActivities;
    }

    static void attach(long context, JavaHost host) {
        HOSTS.put(context, host);
    }

    static void detach(long context) {
        JavaHost host = HOSTS.remove(context);
        if (host != null) {
            host.calls.values().forEach(call -> call.cancel("worker exited", null));
        }
    }

    public static boolean startRpc(long context, long token, Pointer data, int length) {
        JavaHost host = HOSTS.get(context);
        if (host == null) return false;
        try {
            host.start(token, copyFromRust(data, length));
            return true;
        } catch (IOException | RuntimeException e) {
            ClientCall<byte[], byte[]> call = host.calls.remove(token);
            if (call != null) call.cancel("bridge could not start RPC", e);
            host.finish(token, Status.INTERNAL.withDescription(e.toString()), new Metadata(), new Metadata(), null);
            return true;
        }
    }

    public static void cancelRpc(long context, long token) {
        JavaHost host = HOSTS.get(context);
        if (host == null) return;
        ClientCall<byte[], byte[]> call = host.calls.remove(token);
        if (call != null) call.cancel("SDK Core cancelled the RPC", null);
    }

    private void start(long token, byte[] envelope) throws IOException {
        var input = new DataInputStream(new ByteArrayInputStream(envelope));
        String service = new String(readField(input), StandardCharsets.UTF_8);
        String method = new String(readField(input), StandardCharsets.UTF_8);
        Metadata headers = new Metadata();
        CallOptions options = CallOptions.DEFAULT;
        int headerCount = input.readInt();
        for (int i = 0; i < headerCount; i++) {
            String key = new String(readField(input), StandardCharsets.US_ASCII);
            byte[] value = readField(input);
            if (key.equals("grpc-timeout")) {
                options = withTimeout(options, new String(value, StandardCharsets.US_ASCII));
            } else if (!RESERVED_HEADERS.contains(key)) {
                if (key.endsWith("-bin")) {
                    headers.put(Metadata.Key.of(key, Metadata.BINARY_BYTE_MARSHALLER), Base64.getDecoder().decode(value));
                } else {
                    headers.put(Metadata.Key.of(key, Metadata.ASCII_STRING_MARSHALLER),
                        new String(value, StandardCharsets.US_ASCII));
                }
            }
        }
        byte[] payload = readField(input);
        var descriptor = MethodDescriptor.<byte[], byte[]>newBuilder()
            .setFullMethodName(MethodDescriptor.generateFullMethodName(service, method))
            .setType(MethodDescriptor.MethodType.UNARY)
            .setRequestMarshaller(BYTES)
            .setResponseMarshaller(BYTES)
            .build();
        ClientCall<byte[], byte[]> call = channel.newCall(descriptor, options);
        calls.put(token, call);
        call.start(new ClientCall.Listener<>() {
            private Metadata headers = new Metadata();
            private byte[] response;

            @Override public void onHeaders(Metadata received) { headers = received; }
            @Override public void onMessage(byte[] received) { response = received; }

            @Override public void onClose(Status status, Metadata trailers) {
                calls.remove(token, call);
                finish(token, status, headers, trailers, response);
            }
        }, headers);
        call.request(1);
        call.sendMessage(payload);
        call.halfClose();
    }

    private static CallOptions withTimeout(CallOptions options, String value) {
        TimeUnit unit = switch (value.charAt(value.length() - 1)) {
            case 'H' -> TimeUnit.HOURS;
            case 'M' -> TimeUnit.MINUTES;
            case 'S' -> TimeUnit.SECONDS;
            case 'm' -> TimeUnit.MILLISECONDS;
            case 'u' -> TimeUnit.MICROSECONDS;
            case 'n' -> TimeUnit.NANOSECONDS;
            default -> throw new IllegalArgumentException("invalid grpc-timeout unit");
        };
        return options.withDeadlineAfter(Long.parseLong(value.substring(0, value.length() - 1)), unit);
    }

    private void finish(long token, Status status, Metadata headers, Metadata trailers, byte[] response) {
        try {
            var bytes = new ByteArrayOutputStream();
            var output = new DataOutputStream(bytes);
            output.writeInt(status.getCode().value());
            writeField(output, status.getDescription() == null ? null : status.getDescription().getBytes(StandardCharsets.UTF_8));
            writeField(output, trailers.get(STATUS_DETAILS));
            List<Header> metadata = new ArrayList<>();
            addHeaders(metadata, headers);
            addHeaders(metadata, trailers);
            output.writeInt(metadata.size());
            for (Header header : metadata) {
                writeField(output, header.key().getBytes(StandardCharsets.US_ASCII));
                writeField(output, header.value());
            }
            writeField(output, response);
            byte[] envelope = bytes.toByteArray();
            temporal_java_bridge.complete_rpc(token, Pointer.array(envelope, 0, 1), envelope.length);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private record Header(String key, byte[] value) {}

    private static void addHeaders(List<Header> output, Metadata metadata) {
        for (String key : metadata.keys()) {
            if (RESERVED_HEADERS.contains(key)) continue;
            if (key.endsWith("-bin")) {
                for (byte[] value : metadata.getAll(Metadata.Key.of(key, Metadata.BINARY_BYTE_MARSHALLER))) {
                    output.add(new Header(key, Base64.getEncoder().withoutPadding().encode(value)));
                }
            } else {
                for (String value : metadata.getAll(Metadata.Key.of(key, Metadata.ASCII_STRING_MARSHALLER))) {
                    output.add(new Header(key, value.getBytes(StandardCharsets.US_ASCII)));
                }
            }
        }
    }

    public static int handleActivation(long context, Pointer data, int length) {
        JavaHost host = HOSTS.get(context);
        if (host == null) return -1;
        try {
            host.completion = host.handler.activate(copyFromRust(data, length));
            return host.completion == null ? -1 : host.completion.length;
        } catch (Exception e) {
            host.completion = null;
            return -1;
        }
    }

    public static boolean dispatchLocalActivity(long context, Pointer data, int length) {
        JavaHost host = HOSTS.get(context);
        if (host == null || host.localActivities == null) return false;
        try {
            host.localActivities.dispatch(copyFromRust(data, length), completion ->
                temporal_java_bridge.complete_local_activity(
                    context, Pointer.array(completion, 0, 1), completion.length));
            return true;
        } catch (Exception e) {
            host.failure = e;
            return false;
        }
    }

    public static boolean copyCompletion(long context, Pointer destination, int length) {
        JavaHost host = HOSTS.get(context);
        if (host == null) return false;
        byte[] bytes = host.completion;
        host.completion = null;
        if (bytes == null || bytes.length != length) return false;
        if (length > 0) Pointer.copy(Pointer.array(bytes, 0, 1), destination, length);
        return true;
    }

    public static void workerFailed(long context, Pointer data, int length) {
        JavaHost host = HOSTS.get(context);
        if (host != null) {
            host.failure = new IllegalStateException(new String(copyFromRust(data, length), StandardCharsets.UTF_8));
        }
    }

    static byte[] copyFromRust(Pointer source, int length) {
        byte[] bytes = new byte[length];
        if (length > 0) Pointer.copy(source, Pointer.array(bytes, 0, 1), length);
        return bytes;
    }

    static byte[] readField(DataInputStream input) throws IOException {
        int length = input.readInt();
        if (length < 0 || length > input.available()) throw new IOException("truncated bridge field");
        return input.readNBytes(length);
    }

    static void writeField(DataOutputStream output, byte[] data) throws IOException {
        output.writeInt(data == null ? 0 : data.length);
        if (data != null) output.write(data);
    }
}
