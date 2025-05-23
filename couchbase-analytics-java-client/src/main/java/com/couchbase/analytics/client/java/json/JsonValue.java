/*
 * Copyright 2025 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.analytics.client.java.json;

import com.couchbase.analytics.client.java.internal.JsonValueModule;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.jspecify.annotations.Nullable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;

import static com.couchbase.analytics.client.java.internal.utils.lang.CbCollections.listOf;

/**
 * A JSON container, either a {@link JsonObject} or a {@link JsonArray}.
 */
public abstract class JsonValue {
  static final JsonMapper mapper = JsonMapper.builder()
    .addModule(new JsonValueModule())
    .build();

  JsonValue() {
  }

  /**
   * Helper method to check if the given item is a supported JSON item.
   *
   * @param item the value to check.
   * @return true if supported, false otherwise.
   */
  static boolean isSupportedType(@Nullable Object item) {
    return item == null
      || item instanceof String
      || item instanceof Integer
      || item instanceof Long
      || item instanceof Double
      || item instanceof Boolean
      || item instanceof BigInteger
      || item instanceof BigDecimal
      || item instanceof JsonObject
      || item instanceof JsonArray;
  }

  /**
   * Returns the given value converted to a type that passes the {@link #isSupportedType} test.
   *
   * @throws IllegalArgumentException if conversion is not possible.
   */
  @SuppressWarnings("unchecked")
  static @Nullable Object coerce(@Nullable Object value) {
    if (isSupportedType(value)) {
      return value;
    }
    if (value instanceof Map) {
      return JsonObject.from((Map<String, ?>) value);
    }
    if (value instanceof List) {
      return JsonArray.from((List<?>) value);
    }
    if (value instanceof Iterable) {
      return JsonArray.fromIterable((Iterable<?>) value);
    }
    throw new IllegalArgumentException("Unsupported type for JSON value: " + value.getClass() + " ; must be one of " + supportedTypeNames());
  }

  private static List<String> supportedTypeNames() {
    return listOf(
      "String",
      "Integer",
      "Long",
      "Double",
      "Boolean",
      "BigInteger",
      "BigDecimal",
      "JsonObject",
      "JsonArray",
      "Map<String,?>",
      "Iterable<?>",
      "null"
    );
  }
}
