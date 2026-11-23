import io.nexusrpc.Operation;
import io.nexusrpc.Service;
import io.nexusrpc.handler.OperationHandler;
import io.nexusrpc.handler.OperationImpl;
import io.nexusrpc.handler.ServiceImpl;
import io.temporal.activity.Activity;
import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityOptions;
import io.temporal.activity.LocalActivityOptions;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.client.WorkflowStub;
import io.temporal.client.WorkflowUpdateException;
import io.temporal.common.RetryOptions;
import io.temporal.failure.ApplicationFailure;
import io.temporal.nexus.Nexus;
import io.temporal.nexus.WorkflowRunOperation;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.serviceclient.WorkflowServiceStubsOptions;
import io.temporal.worker.WorkerFactory;
import io.temporal.workflow.NexusOperationOptions;
import io.temporal.workflow.NexusServiceOptions;
import io.temporal.workflow.SignalMethod;
import io.temporal.workflow.UpdateMethod;
import io.temporal.workflow.UpdateValidatorMethod;
import io.temporal.workflow.Workflow;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class CoreActivationProbe {
  private static final String QUEUE = "core-activation-probe";
  private static final String ENDPOINT = "core-activation-probe";

  @WorkflowInterface
  public interface Parent {
    @WorkflowMethod
    String run(String target);

    @UpdateMethod
    String change(String value);

    @UpdateValidatorMethod(updateName = "change")
    void validate(String value);

    @SignalMethod
    void finish();
  }

  @WorkflowInterface
  public interface Child {
    @WorkflowMethod
    String run(String value);
  }

  @WorkflowInterface
  public interface Continued {
    @WorkflowMethod
    int run(int iteration);
  }

  @WorkflowInterface
  public interface Target {
    @WorkflowMethod
    void run();

    @SignalMethod
    void mark(String value);
  }

  @ActivityInterface
  public interface EchoActivity {
    String echo(String value);
  }

  @Service
  public interface EchoService {
    @Operation
    String sync(String value);

    @Operation
    String async(String value);
  }

  public static class ParentImpl implements Parent {
    private String value;
    private boolean finished;

    @Override
    public String run(String target) {
      int version = Workflow.getVersion("activation-probe", Workflow.DEFAULT_VERSION, 2);
      EchoActivity remote =
          Workflow.newActivityStub(
              EchoActivity.class,
              ActivityOptions.newBuilder().setStartToCloseTimeout(Duration.ofSeconds(5)).build());
      EchoActivity local =
          Workflow.newLocalActivityStub(
              EchoActivity.class,
              LocalActivityOptions.newBuilder()
                  .setStartToCloseTimeout(Duration.ofSeconds(5))
                  .setLocalRetryThreshold(Duration.ofMillis(20))
                  .setRetryOptions(
                      RetryOptions.newBuilder()
                          .setInitialInterval(Duration.ofMillis(200))
                          .setMaximumAttempts(2)
                          .build())
                  .build());
      String remoteResult = remote.echo("remote");
      String localResult = local.echo("local");
      String childResult = Workflow.newChildWorkflowStub(Child.class).run("child");
      Workflow.newExternalWorkflowStub(Target.class, target).mark("sent");
      Workflow.newUntypedExternalWorkflowStub(target).cancel();
      EchoService nexus =
          Workflow.newNexusServiceStub(
              EchoService.class,
              NexusServiceOptions.newBuilder()
                  .setEndpoint(ENDPOINT)
                  .setOperationOptions(
                      NexusOperationOptions.newBuilder()
                          .setScheduleToCloseTimeout(Duration.ofSeconds(15))
                          .build())
                  .build());
      String syncResult = nexus.sync("sync");
      var operation = Workflow.startNexusOperation(nexus::async, "async");
      if (operation.getExecution().get().getOperationToken().isEmpty()) {
        throw ApplicationFailure.newNonRetryableFailure("Missing Nexus operation token", "Probe");
      }
      String asyncResult = operation.getResult().get();
      Workflow.await(() -> value != null && finished);
      return String.join(
          "|",
          Integer.toString(version),
          remoteResult,
          localResult,
          childResult,
          syncResult,
          asyncResult,
          value);
    }

    @Override
    public String change(String value) {
      this.value = value;
      return value.toUpperCase(java.util.Locale.ROOT);
    }

    @Override
    public void validate(String value) {
      if (value.isBlank()) throw new IllegalArgumentException("blank update");
    }

    @Override
    public void finish() {
      finished = true;
    }
  }

  public static class ChildImpl implements Child {
    @Override
    public String run(String value) {
      return value;
    }
  }

  public static class ContinuedImpl implements Continued {
    @Override
    public int run(int iteration) {
      if (iteration == 0) {
        Workflow.continueAsNew(iteration + 1);
      }
      return iteration;
    }
  }

  public static class TargetImpl implements Target {
    @Override
    public void run() {
      Workflow.await(() -> false);
    }

    @Override
    public void mark(String value) {}
  }

  public static class EchoActivityImpl implements EchoActivity {
    @Override
    public String echo(String value) {
      var info = Activity.getExecutionContext().getInfo();
      if (info.isLocal() && info.getAttempt() == 1) {
        throw ApplicationFailure.newFailure("exercise Core local retry backoff", "Probe");
      }
      return value;
    }
  }

  @ServiceImpl(service = EchoService.class)
  public static class EchoServiceImpl {
    @OperationImpl
    public OperationHandler<String, String> sync() {
      return OperationHandler.sync((ctx, details, value) -> value);
    }

    @OperationImpl
    public OperationHandler<String, String> async() {
      return WorkflowRunOperation.fromWorkflowMethod(
          (ctx, details, value) ->
              Nexus.getOperationContext()
                      .getWorkflowClient()
                      .newWorkflowStub(
                          Child.class,
                          WorkflowOptions.newBuilder()
                              .setWorkflowId(details.getRequestId())
                              .build())
                  ::run);
    }
  }

  public static void main(String[] args) throws InterruptedException {
    WorkflowServiceStubs service =
        WorkflowServiceStubs.newServiceStubs(
            WorkflowServiceStubsOptions.newBuilder().setTarget(args[0]).build());
    WorkflowClient client = WorkflowClient.newInstance(service);
    if (args[1].equals("worker")) {
      WorkerFactory factory = WorkerFactory.newInstance(client);
      var worker = factory.newWorker(QUEUE);
      worker.registerWorkflowImplementationTypes(
          ParentImpl.class, ChildImpl.class, TargetImpl.class, ContinuedImpl.class);
      worker.registerActivitiesImplementations(new EchoActivityImpl());
      worker.registerNexusServiceImplementation(new EchoServiceImpl());
      Runtime.getRuntime()
          .addShutdownHook(
              new Thread(
                  () -> {
                    factory.shutdown();
                    factory.awaitTermination(15, TimeUnit.SECONDS);
                    service.shutdownNow();
                  }));
      factory.start();
      new CountDownLatch(1).await();
    } else if (args[1].equals("client")) {
      String id = args[2];
      try {
        var target =
            client.newWorkflowStub(
                Target.class,
                WorkflowOptions.newBuilder()
                    .setWorkflowId(id + "-target")
                    .setTaskQueue(QUEUE)
                    .build());
        WorkflowClient.start(target::run);
        var parent =
            client.newWorkflowStub(
                Parent.class,
                WorkflowOptions.newBuilder().setWorkflowId(id).setTaskQueue(QUEUE).build());
        WorkflowClient.start(parent::run, id + "-target");
        try {
          parent.change("");
          throw new AssertionError("The update validator accepted a blank value");
        } catch (WorkflowUpdateException expected) {
          System.out.println("Blank update rejected");
        }
        if (!parent.change("updated").equals("UPDATED")) {
          throw new AssertionError("Unexpected update result");
        }
        var result = WorkflowStub.fromTyped(parent).getResult(String.class);
        if (!result.equals("2|remote|local|child|sync|async|updated")) {
          throw new AssertionError("Unexpected workflow result " + result);
        }
        var continued =
            client.newWorkflowStub(
                Continued.class,
                WorkflowOptions.newBuilder()
                    .setWorkflowId(id + "-continued")
                    .setTaskQueue(QUEUE)
                    .build());
        if (continued.run(0) != 1) {
          throw new AssertionError("Continue-as-new did not start the next iteration");
        }
        System.out.println("Core activation probe completed with " + result);
      } finally {
        service.shutdownNow();
        service.awaitTermination(5, TimeUnit.SECONDS);
      }
    } else {
      throw new IllegalArgumentException("Expected worker or client");
    }
  }
}
