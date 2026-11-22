package io.temporal.serviceclient;

/** Connection options for the plaintext service client. */
public final class WorkflowServiceStubsOptions {
    private final String target;

    private WorkflowServiceStubsOptions(String target) {
        this.target = target;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static WorkflowServiceStubsOptions getDefaultInstance() {
        return newBuilder().build();
    }

    public String getTarget() {
        return target;
    }

    public static final class Builder {
        private String target = "127.0.0.1:7233";

        private Builder() {}

        public Builder setTarget(String target) {
            if (target == null || target.isBlank()) {
                throw new IllegalArgumentException("Target is required");
            }
            this.target = target;
            return this;
        }

        public WorkflowServiceStubsOptions build() {
            return new WorkflowServiceStubsOptions(target);
        }
    }
}
