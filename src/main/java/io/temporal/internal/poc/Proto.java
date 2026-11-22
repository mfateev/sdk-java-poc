package io.temporal.internal.poc;

import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

final class Proto {
    static final String API = "temporal.api.";
    static final String CORE = "coresdk.";
    private static final Map<String, Descriptor> TYPES = loadTypes();

    private Proto() {}

    static Descriptor type(String name) {
        return Objects.requireNonNull(TYPES.get(name), name);
    }

    static DynamicMessage.Builder builder(String type) {
        return DynamicMessage.newBuilder(type(type));
    }

    static DynamicMessage message(String type, String field, Object value) {
        return set(builder(type), field, value).build();
    }

    static DynamicMessage.Builder set(DynamicMessage.Builder builder, String field, Object value) {
        return builder.setField(field(builder.getDescriptorForType(), field), value);
    }

    static DynamicMessage.Builder setEnum(DynamicMessage.Builder builder, String field, String value) {
        FieldDescriptor descriptor = field(builder.getDescriptorForType(), field);
        return builder.setField(descriptor, descriptor.getEnumType().findValueByName(value));
    }

    static Object get(DynamicMessage message, String field) {
        return message.getField(field(message.getDescriptorForType(), field));
    }

    static boolean has(DynamicMessage message, String field) {
        return message.hasField(field(message.getDescriptorForType(), field));
    }

    @SuppressWarnings("unchecked")
    static List<DynamicMessage> messages(DynamicMessage message, String field) {
        return (List<DynamicMessage>) get(message, field);
    }

    private static FieldDescriptor field(Descriptor type, String name) {
        return Objects.requireNonNull(type.findFieldByName(name), type.getFullName() + "." + name);
    }

    private static Map<String, Descriptor> loadTypes() {
        try (var input = Proto.class.getResourceAsStream("/temporal/poc/schema.pb")) {
            var descriptions = new HashMap<String, FileDescriptorProto>();
            for (var file : FileDescriptorSet.parseFrom(Objects.requireNonNull(input)).getFileList()) {
                descriptions.put(file.getName(), file);
            }
            var files = new HashMap<String, FileDescriptor>();
            var types = new HashMap<String, Descriptor>();
            for (String name : descriptions.keySet()) {
                for (var type : loadFile(name, descriptions, files).getMessageTypes()) {
                    types.put(type.getFullName(), type);
                }
            }
            return Map.copyOf(types);
        } catch (IOException | DescriptorValidationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private static FileDescriptor loadFile(
            String name, Map<String, FileDescriptorProto> descriptions, Map<String, FileDescriptor> files)
            throws DescriptorValidationException {
        FileDescriptor existing = files.get(name);
        if (existing != null) {
            return existing;
        }
        FileDescriptorProto file = descriptions.get(name);
        var dependencies = new FileDescriptor[file.getDependencyCount()];
        for (int i = 0; i < dependencies.length; i++) {
            dependencies[i] = loadFile(file.getDependency(i), descriptions, files);
        }
        FileDescriptor descriptor = FileDescriptor.buildFrom(file, dependencies);
        files.put(name, descriptor);
        return descriptor;
    }
}
