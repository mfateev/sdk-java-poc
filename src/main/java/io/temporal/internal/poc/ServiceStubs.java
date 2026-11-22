package io.temporal.internal.poc;

import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.channel.nio.NioEventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.socket.nio.NioSocketChannel;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.serviceclient.WorkflowServiceStubsOptions;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public final class ServiceStubs implements WorkflowServiceStubs {
    private final NioEventLoopGroup eventLoop;
    private final ManagedChannel channel;

    public ServiceStubs(WorkflowServiceStubsOptions options) {
        String target = Objects.requireNonNull(options).getTarget();
        eventLoop = new NioEventLoopGroup(1);
        try {
            channel = NettyChannelBuilder.forTarget(target)
                .channelType(NioSocketChannel.class)
                .eventLoopGroup(eventLoop)
                .disableRetry()
                .usePlaintext()
                .build();
        } catch (RuntimeException | Error e) {
            eventLoop.shutdownGracefully(0, 0, TimeUnit.SECONDS);
            throw e;
        }
    }

    @Override public ManagedChannel getRawChannel() {
        return channel;
    }

    @Override public void shutdown() {
        channel.shutdown();
        stopEventLoopWhenChannelTerminates();
    }

    @Override public void shutdownNow() {
        channel.shutdownNow();
        stopEventLoopWhenChannelTerminates();
    }

    private synchronized void stopEventLoopWhenChannelTerminates() {
        if (shutdownStarted) {
            return;
        }
        shutdownStarted = true;
        Thread.startVirtualThread(() -> {
            try {
                channel.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                eventLoop.shutdownGracefully(0, 0, TimeUnit.SECONDS);
            }
        });
    }

    private boolean shutdownStarted;

    @Override public boolean isShutdown() {
        return channel.isShutdown();
    }

    @Override public boolean isTerminated() {
        return channel.isTerminated() && eventLoop.isTerminated();
    }

    @Override public boolean awaitTermination(long timeout, TimeUnit unit) {
        try {
            long nanos = unit.toNanos(timeout);
            long start = System.nanoTime();
            return channel.awaitTermination(nanos, TimeUnit.NANOSECONDS)
                && eventLoop.awaitTermination(Math.max(0, nanos - (System.nanoTime() - start)), TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }
}
