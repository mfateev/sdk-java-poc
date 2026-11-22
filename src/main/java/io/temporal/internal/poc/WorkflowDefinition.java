package io.temporal.internal.poc;

import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;

public record WorkflowDefinition(String name, Method method, Constructor<?> constructor) {
    public static WorkflowDefinition forInterface(Class<?> type) {
        if (!type.isInterface() || !Modifier.isPublic(type.getModifiers())
                || !type.isAnnotationPresent(WorkflowInterface.class)) {
            throw new IllegalArgumentException("Expected a public @WorkflowInterface: " + type.getName());
        }
        var methods = Arrays.stream(type.getMethods())
            .filter(m -> m.isAnnotationPresent(WorkflowMethod.class))
            .toList();
        if (methods.size() != 1) {
            throw new IllegalArgumentException("Expected exactly one @WorkflowMethod on " + type.getName());
        }
        Method method = methods.getFirst();
        Class<?> result = method.getReturnType();
        if (Modifier.isStatic(method.getModifiers()) || (result != String.class && result != void.class)
                || Arrays.stream(method.getParameterTypes()).anyMatch(parameter -> parameter != String.class)) {
            throw new IllegalArgumentException("The timer POC accepts String arguments and String or void results");
        }
        String name = method.getAnnotation(WorkflowMethod.class).name();
        if (name.isEmpty()) {
            Class<?> declaring = method.getDeclaringClass();
            name = (declaring.isAnnotationPresent(WorkflowInterface.class) ? declaring : type).getSimpleName();
        }
        return new WorkflowDefinition(name, method, null);
    }

    public static WorkflowDefinition forImplementation(Class<?> type) {
        Set<Class<?>> interfaces = new LinkedHashSet<>();
        collectInterfaces(type, interfaces);
        if (!Modifier.isPublic(type.getModifiers()) || Modifier.isAbstract(type.getModifiers())
                || interfaces.size() != 1) {
            throw new IllegalArgumentException("Expected a public implementation of one workflow interface: " + type.getName());
        }
        WorkflowDefinition definition = forInterface(interfaces.iterator().next());
        try {
            return new WorkflowDefinition(definition.name, definition.method, type.getConstructor());
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException("A public no-argument workflow constructor is required", e);
        }
    }

    private static void collectInterfaces(Class<?> type, Set<Class<?>> result) {
        if (type == null) {
            return;
        }
        for (Class<?> candidate : type.getInterfaces()) {
            if (candidate.isAnnotationPresent(WorkflowInterface.class)) {
                result.add(candidate);
            } else {
                collectInterfaces(candidate, result);
            }
        }
        collectInterfaces(type.getSuperclass(), result);
    }
}
