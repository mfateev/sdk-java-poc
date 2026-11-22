package io.temporal.workflow;

import io.temporal.internal.poc.WorkflowRunner;
import java.time.Duration;

/** Operations available to workflow code. */
public final class Workflow {
    private Workflow() {}

    /** Suspends the workflow until a durable timer fires. */
    public static void sleep(Duration duration) {
        WorkflowRunner.sleep(duration);
    }

    public static void sleep(long millis) {
        sleep(Duration.ofMillis(millis));
    }
}
