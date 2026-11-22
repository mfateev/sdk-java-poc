package io.temporal.internal.poc;

import static io.temporal.internal.poc.Proto.API;
import static io.temporal.internal.poc.Proto.CORE;

import com.google.protobuf.DynamicMessage;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public final class WorkflowRunner implements AutoCloseable {
    private static final String ACTIVATION = CORE + "workflow_activation.WorkflowActivation";
    private static final String COMPLETION = CORE + "workflow_completion.WorkflowActivationCompletion";
    private static final String COMMANDS = CORE + "workflow_commands.";
    private static final ThreadLocal<Run> CURRENT = new ThreadLocal<>();
    private static final System.Logger LOGGER = System.getLogger(WorkflowRunner.class.getName());

    private final Map<String, WorkflowDefinition> definitions;
    private final Map<String, Run> runs = new HashMap<>();

    public WorkflowRunner(Map<String, WorkflowDefinition> definitions) {
        this.definitions = Map.copyOf(definitions);
    }

    public static void sleep(Duration duration) {
        Objects.requireNonNull(duration);
        Run run = CURRENT.get();
        if (run == null) {
            throw new IllegalStateException("Workflow.sleep must be called from a workflow method");
        }
        if (duration.isNegative()) {
            throw new IllegalArgumentException("Sleep duration must be nonnegative");
        }
        if (!duration.isZero()) {
            run.sleep(duration);
        }
    }

    public byte[] activate(byte[] bytes) throws IOException {
        DynamicMessage activation = DynamicMessage.parseFrom(Proto.type(ACTIVATION), bytes);
        String runId = (String) Proto.get(activation, "run_id");
        var completion = Proto.builder(COMPLETION);
        Proto.set(completion, "run_id", runId);
        var commands = new ArrayList<DynamicMessage>();
        try {
            for (DynamicMessage job : Proto.messages(activation, "jobs")) {
                var variant = job.getOneofFieldDescriptor(job.getDescriptorForType().getOneofs().getFirst());
                if (variant == null) {
                    throw new IllegalArgumentException("Unsupported workflow activation job");
                }
                DynamicMessage data = (DynamicMessage) job.getField(variant);
                switch (variant.getName()) {
                    case "initialize_workflow" -> commands.add(step(runId, initialize(runId, data)));
                    case "fire_timer" -> {
                        Run run = runs.get(runId);
                        if (run == null || run.pendingTimer != (Integer) Proto.get(data, "seq")) {
                            throw new IllegalStateException("Timer doesn't match the suspended workflow for " + runId);
                        }
                        run.resume.release();
                        commands.add(step(runId, run));
                    }
                    case "remove_from_cache" -> remove(runId);
                    default -> throw new IllegalArgumentException("Unsupported workflow activation " + variant.getName());
                }
            }
            Proto.set(completion, "successful",
                Proto.message(CORE + "workflow_completion.Success", "commands", commands));
        } catch (Exception e) {
            remove(runId);
            LOGGER.log(System.Logger.Level.ERROR, "Workflow activation failed for " + runId, e);
            var failure = Proto.message(API + "failure.v1.Failure", "message", e.toString());
            Proto.set(completion, "failed", Proto.message(CORE + "workflow_completion.Failure", "failure", failure));
        }
        return completion.build().toByteArray();
    }

    private Run initialize(String runId, DynamicMessage init) throws IOException {
        String workflowType = (String) Proto.get(init, "workflow_type");
        WorkflowDefinition definition = definitions.get(workflowType);
        if (definition == null) {
            throw new IllegalArgumentException("No workflow registered for " + workflowType);
        }
        var input = Proto.messages(init, "arguments");
        if (input.size() != definition.method().getParameterCount() || runs.containsKey(runId)) {
            throw new IllegalStateException("Invalid initialization of workflow run " + runId);
        }
        var args = new Object[input.size()];
        for (int i = 0; i < args.length; i++) {
            args[i] = Payloads.decode(input.get(i));
        }
        Run run = new Run(runId, definition, args);
        runs.put(runId, run);
        run.thread.start();
        return run;
    }

    private DynamicMessage step(String runId, Run run) throws Exception {
        Step step;
        try {
            step = run.next.poll(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw e;
        }
        return switch (step) {
            case Timer timer -> {
                run.pendingTimer = ++run.sequence;
                Duration duration = timer.duration();
                var timeout = Proto.builder("google.protobuf.Duration");
                Proto.set(timeout, "seconds", duration.getSeconds());
                Proto.set(timeout, "nanos", duration.getNano());
                var startTimer = Proto.builder(COMMANDS + "StartTimer");
                Proto.set(startTimer, "seq", run.pendingTimer);
                Proto.set(startTimer, "start_to_fire_timeout", timeout.build());
                yield Proto.message(COMMANDS + "WorkflowCommand", "start_timer", startTimer.build());
            }
            case Returned returned -> {
                runs.remove(runId);
                var complete = Proto.builder(COMMANDS + "CompleteWorkflowExecution");
                if (run.hasResult) {
                    Proto.set(complete, "result", Payloads.encode(returned.value()));
                }
                yield Proto.message(COMMANDS + "WorkflowCommand", "complete_workflow_execution", complete.build());
            }
            case Failed failed -> throw new IllegalStateException("Workflow code failed", failed.error());
            case null -> throw new IllegalStateException("Workflow did not return or suspend within five seconds");
        };
    }

    private void remove(String runId) {
        Run run = runs.remove(runId);
        if (run != null) {
            run.thread.interrupt();
        }
    }

    @Override public void close() {
        runs.values().forEach(run -> run.thread.interrupt());
        runs.clear();
    }

    private sealed interface Step {}
    private record Timer(Duration duration) implements Step {}
    private record Returned(String value) implements Step {}
    private record Failed(Throwable error) implements Step {}

    private static final class Run {
        final ArrayBlockingQueue<Step> next = new ArrayBlockingQueue<>(1);
        final Semaphore resume = new Semaphore(0);
        final Thread thread;
        final boolean hasResult;
        int sequence;
        int pendingTimer;

        Run(String runId, WorkflowDefinition definition, Object[] args) {
            hasResult = definition.method().getReturnType() != void.class;
            thread = Thread.ofVirtual().name("workflow-" + runId).unstarted(() -> execute(definition, args));
        }

        void sleep(Duration duration) {
            next.add(new Timer(duration));
            try {
                resume.acquire();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Workflow run was evicted", e);
            }
        }

        private void execute(WorkflowDefinition definition, Object[] args) {
            CURRENT.set(this);
            try {
                Object instance = definition.constructor().newInstance();
                next.add(new Returned((String) definition.method().invoke(instance, args)));
            } catch (InvocationTargetException e) {
                next.offer(new Failed(e.getCause()));
            } catch (Throwable e) {
                next.offer(new Failed(e));
            } finally {
                CURRENT.remove();
            }
        }
    }
}
