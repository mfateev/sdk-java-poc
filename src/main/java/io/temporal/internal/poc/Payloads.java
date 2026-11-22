package io.temporal.internal.poc;

import static io.temporal.internal.poc.Proto.API;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.io.JsonStringEncoder;
import com.google.protobuf.ByteString;
import com.google.protobuf.DynamicMessage;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

final class Payloads {
    private static final String PAYLOAD = API + "common.v1.Payload";
    private static final ByteString JSON = ByteString.copyFromUtf8("json/plain");
    private static final ByteString NULL = ByteString.copyFromUtf8("binary/null");
    private static final JsonFactory JSON_FACTORY = new JsonFactory();

    private Payloads() {}

    static DynamicMessage encodeArguments(Object[] values) {
        List<DynamicMessage> payloads = new ArrayList<>(values.length);
        for (Object value : values) {
            payloads.add(encode((String) value));
        }
        return Proto.message(API + "common.v1.Payloads", "payloads", payloads);
    }

    static DynamicMessage encode(String value) {
        var payload = Proto.builder(PAYLOAD);
        var metadata = DynamicMessage.newBuilder(payload.getDescriptorForType().findNestedTypeByName("MetadataEntry"));
        Proto.set(metadata, "key", "encoding");
        Proto.set(metadata, "value", value == null ? NULL : JSON);
        Proto.set(payload, "metadata", List.of(metadata.build()));
        if (value != null) {
            byte[] quoted = JsonStringEncoder.getInstance().quoteAsUTF8(value);
            ByteString quote = ByteString.copyFromUtf8("\"");
            Proto.set(payload, "data", quote.concat(ByteString.copyFrom(quoted)).concat(quote));
        }
        return payload.build();
    }

    static String decode(DynamicMessage payload) throws IOException {
        ByteString encoding = ByteString.EMPTY;
        for (DynamicMessage metadata : Proto.messages(payload, "metadata")) {
            if (Proto.get(metadata, "key").equals("encoding")) {
                encoding = (ByteString) Proto.get(metadata, "value");
                break;
            }
        }
        if (encoding.equals(NULL)) {
            return null;
        }
        if (!encoding.equals(JSON)) {
            throw new IOException("Unsupported payload encoding " + encoding.toStringUtf8());
        }
        try (var parser = JSON_FACTORY.createParser(((ByteString) Proto.get(payload, "data")).newInput())) {
            if (parser.nextToken() != JsonToken.VALUE_STRING) {
                throw new IOException("Expected a JSON string payload");
            }
            String result = parser.getText();
            if (parser.nextToken() != null) {
                throw new IOException("Unexpected data after string payload");
            }
            return result;
        }
    }
}
