import coresdk.workflow_activation.WorkflowActivationOuterClass.InitializeWorkflow;
import io.temporal.api.common.v1.WorkflowExecution;
import io.temporal.api.history.v1.WorkflowExecutionStartedEventAttributes;
import io.temporal.internal.replay.ReplayWorkflowContext;
import io.temporal.internal.worker.SingleWorkerOptions;
import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/** Executable contract check for the Java runner's Core workflow context. */
final class VerifyReplayWorkflowContext {
  private static final Set<String> UNIMPLEMENTED = Set.of("sideEffect", "mutableSideEffect");

  public static void main(String[] args) throws ReflectiveOperationException {
    Class<?> contract = ReplayWorkflowContext.class;
    Class<?> implementation =
        Class.forName(contract.getPackageName() + ".CoreReplayWorkflowContext");
    if (!contract.isAssignableFrom(implementation)) {
      throw new AssertionError(
          implementation.getName() + " does not implement " + contract.getName());
    }

    var expected = methods(contract);
    var actual = methods(implementation);
    if (!actual.equals(expected)) {
      var missing = new TreeSet<>(expected);
      missing.removeAll(actual);
      var additional = new TreeSet<>(actual);
      additional.removeAll(expected);
      throw new AssertionError(
          "Core workflow context methods differ. Missing "
              + missing
              + ", additional "
              + additional);
    }
    for (Method method : contract.getMethods()) {
      if (contextMethod(method)
          && implementation
                  .getMethod(method.getName(), method.getParameterTypes())
                  .getDeclaringClass()
              != implementation) {
        throw new AssertionError("Missing explicit Core implementation of " + method);
      }
    }

    var constructor =
        implementation.getDeclaredConstructor(
            String.class,
            WorkflowExecution.class,
            WorkflowExecutionStartedEventAttributes.class,
            InitializeWorkflow.class,
            SingleWorkerOptions.class);
    constructor.setAccessible(true);
    Object context =
        constructor.newInstance(
            "default",
            WorkflowExecution.newBuilder().setWorkflowId("workflow").setRunId("run").build(),
            WorkflowExecutionStartedEventAttributes.getDefaultInstance(),
            InitializeWorkflow.getDefaultInstance(),
            SingleWorkerOptions.newBuilder().build());
    Class<?> unimplemented = Class.forName(contract.getPackageName() + ".UnimplementedException");

    var checked = new TreeSet<String>();
    for (Method method : contract.getMethods()) {
      if (!UNIMPLEMENTED.contains(method.getName())) {
        continue;
      }
      Object[] arguments =
          Arrays.stream(method.getParameterTypes())
              .map(type -> type.isPrimitive() ? Array.get(Array.newInstance(type, 1), 0) : null)
              .toArray();
      try {
        method.invoke(context, arguments);
        throw new AssertionError(method.getName() + " returned instead of throwing");
      } catch (InvocationTargetException error) {
        Throwable cause = error.getCause();
        if (!unimplemented.isInstance(cause)
            || !cause.getMessage().startsWith(method.getName() + " ")) {
          throw new AssertionError(method.getName() + " threw the wrong exception", cause);
        }
      }
      checked.add(method.getName());
    }
    if (!checked.equals(UNIMPLEMENTED)) {
      throw new AssertionError(
          "Some unimplemented operations are absent from the SDK Java contract");
    }
    System.out.printf(
        "Verified %d context methods and %d unimplemented operations.%n",
        expected.size(), checked.size());
  }

  private static Set<String> methods(Class<?> type) {
    return Arrays.stream(type.getMethods())
        .filter(VerifyReplayWorkflowContext::contextMethod)
        .map(VerifyReplayWorkflowContext::signature)
        .collect(Collectors.toCollection(TreeSet::new));
  }

  private static boolean contextMethod(Method method) {
    return method.getDeclaringClass() != Object.class
        && !method.isSynthetic()
        && !Modifier.isStatic(method.getModifiers());
  }

  private static String signature(Method method) {
    String typeParameters =
        Arrays.stream(method.getTypeParameters())
            .map(type -> type.getName() + " extends " + typeNames(type.getBounds()))
            .collect(Collectors.joining(", ", "<", ">"));
    return typeParameters
        + " "
        + method.getGenericReturnType().getTypeName()
        + " "
        + method.getName()
        + "("
        + typeNames(method.getGenericParameterTypes())
        + ")"
        + (method.isVarArgs() ? " (varargs)" : "")
        + " throws "
        + typeNames(method.getGenericExceptionTypes());
  }

  private static String typeNames(Type[] types) {
    return Arrays.stream(types).map(Type::getTypeName).collect(Collectors.joining(", "));
  }
}
