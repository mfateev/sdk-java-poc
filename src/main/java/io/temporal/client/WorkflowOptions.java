package io.temporal.client;

/** Options for starting a workflow. */
public final class WorkflowOptions {
    private final String workflowId;
    private final String taskQueue;

    private WorkflowOptions(Builder builder) {
        workflowId = builder.workflowId;
        taskQueue = builder.taskQueue;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public String getWorkflowId() {
        return workflowId;
    }

    public String getTaskQueue() {
        return taskQueue;
    }

    public static final class Builder {
        private String workflowId;
        private String taskQueue;

        private Builder() {}

        public Builder setWorkflowId(String workflowId) {
            this.workflowId = workflowId;
            return this;
        }

        public Builder setTaskQueue(String taskQueue) {
            this.taskQueue = taskQueue;
            return this;
        }

        public WorkflowOptions build() {
            return new WorkflowOptions(this);
        }
    }
}
