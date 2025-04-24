package com.couchbase.analytics.fit.performer.util;

import com.couchbase.analytics.client.java.codec.Deserializer;
import com.couchbase.analytics.client.java.codec.TypeRef;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.lang.reflect.Type;

/**
 * A custom deserializer for FIT that sneakily adds a field to the result
 * so the driver can verify the SDK honored the request to use a custom deserializer.
 */
public class CustomDeserializer implements Deserializer {
  private static final JsonMapper mapper = JsonMapper.builder().build();

  @Override
  public <T> T deserialize(Class<T> target, byte[] input) {
    requireObjectNode(target);

    try {
      ObjectNode obj = (ObjectNode) mapper.readTree(input);
      obj.put("Serialized", false);
      return (T) obj;

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public <T> T deserialize(TypeRef<T> target, byte[] input) {
    requireObjectNode(target.type());
    return (T) deserialize(ObjectNode.class, input);
  }

  private static void requireObjectNode(Type type) {
    if (!type.equals(ObjectNode.class)) {
      throw new UnsupportedOperationException("Expected target class to be ObjectNode, but got: " + type);
    }
  }
}
