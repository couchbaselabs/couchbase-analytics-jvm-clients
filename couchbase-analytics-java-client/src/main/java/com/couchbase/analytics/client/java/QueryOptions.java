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

package com.couchbase.analytics.client.java;

import com.couchbase.analytics.client.java.codec.Deserializer;
import com.couchbase.analytics.client.java.internal.InternalJacksonSerDes;
import com.couchbase.analytics.client.java.internal.JsonSerializer;
import com.couchbase.analytics.client.java.internal.utils.json.Mapper;
import com.couchbase.analytics.client.java.internal.utils.time.GolangDuration;
import com.couchbase.analytics.client.java.json.JsonArray;
import com.couchbase.analytics.client.java.json.JsonObject;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.jetbrains.annotations.ApiStatus.Experimental;
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;

/**
 * Optional parameters for {@link Queryable#executeQuery(String, Consumer)}
 * and {@link Queryable#executeStreamingQuery(String, Consumer, Consumer)}.
 */
public final class QueryOptions {
  private @Nullable Duration timeout;
  private @Nullable String clientContextId;
  private @Nullable Map<String, ?> namedParameters;
  private @Nullable List<?> positionalParameters;
  private @Nullable ScanConsistency scanConsistency;
  private @Nullable Duration scanWait;
  private @Nullable Deserializer deserializer;
  private @Nullable Boolean readOnly;
  private @Nullable Map<String, ?> raw;
  private @Nullable Integer maxRetries;

  QueryOptions() {
  }

  public QueryOptions timeout(@Nullable Duration timeout) {
    this.timeout = timeout;
    return this;
  }

  public QueryOptions clientContextId(@Nullable String clientContextId) {
    this.clientContextId = clientContextId;
    return this;
  }

  public QueryOptions parameters(@Nullable Map<String, ?> namedParameters) {
    this.namedParameters = namedParameters == null ? null : unmodifiableMap(new HashMap<>(namedParameters));
    return this;
  }

  /**
   * @implNote There must not be a varargs overload; that would
   * introduce ambiguity, since a query can have a single parameter
   * whose value is a list.
   */
  public QueryOptions parameters(@Nullable List<?> positionalParameters) {
    this.positionalParameters = positionalParameters == null ? null : unmodifiableList(new ArrayList<>(positionalParameters));
    return this;
  }

  /**
   * Sets the deserializer used by {@link Row#as} to convert query result rows into Java objects.
   * <p>
   * If not specified, defaults to the cluster's default deserializer.
   *
   * @see ClusterOptions#deserializer(Deserializer)
   */
  public QueryOptions deserializer(@Nullable Deserializer deserializer) {
    this.deserializer = deserializer;
    return this;
  }

  public QueryOptions scanWait(@Nullable Duration scanWait) {
    this.scanWait = scanWait;
    return this;
  }

  public QueryOptions scanConsistency(@Nullable ScanConsistency scanConsistency) {
    this.scanConsistency = scanConsistency;
    return this;
  }

  public QueryOptions readOnly(@Nullable Boolean readOnly) {
    this.readOnly = readOnly;
    return this;
  }

  /**
   * Limits the number of times a failed retriable request is retried.
   * <p>
   * If not specified, defaults to the cluster's default max retries.
   *
   * @see ClusterOptions#maxRetries(Integer)
   */
  @Experimental
  public QueryOptions maxRetries(@Nullable Integer maxRetries) {
    this.maxRetries = maxRetries;
    return this;
  }

  /**
   * Specifies arbitrary name-value pairs to include the query request JSON.
   * <p>
   * Marked as "experimental" because this might not be supported by future
   * client-server protocols. Let us know if you need to use this method,
   * so we can consider extending the stable API to support your use case.
   */
  @Experimental
  public QueryOptions raw(@Nullable Map<String, ?> raw) {
    this.raw = raw == null ? null : unmodifiableMap(new HashMap<>(raw));
    return this;
  }

  Unmodifiable build() {
    return new Unmodifiable(this);
  }

  static class Unmodifiable {
    private final @Nullable Duration timeout;
    private final @Nullable String clientContextId;
    private final @Nullable Map<String, ?> namedParameters;
    private final @Nullable List<?> positionalParameters;
    private final @Nullable ScanConsistency scanConsistency;
    private final @Nullable Duration scanWait;
    private final @Nullable Deserializer deserializer;
    private final @Nullable Boolean readOnly;
    private final @Nullable Map<String, ?> raw;
    private final @Nullable Integer maxRetries;

    Unmodifiable(QueryOptions builder) {
      this.timeout = builder.timeout;
      this.clientContextId = builder.clientContextId;
      this.namedParameters = builder.namedParameters;
      this.positionalParameters = builder.positionalParameters;
      this.scanConsistency = builder.scanConsistency;
      this.scanWait = builder.scanWait;
      this.deserializer = builder.deserializer;
      this.readOnly = builder.readOnly;
      this.raw = builder.raw;
      this.maxRetries = builder.maxRetries;
    }

    void injectParams(ObjectNode query) {
      query.put(
        "client_context_id",
        // Generating the random ID late like this means a new ID is assigned to each retry.
        clientContextId != null ? clientContextId : UUID.randomUUID().toString()
      );

      if (scanConsistency != null) {
        query.put("scan_consistency", scanConsistency.toString());
      }

      if (
        scanWait != null
          && scanConsistency != null
          && scanConsistency != ScanConsistency.NOT_BOUNDED
      ) {
        query.put("scan_wait", GolangDuration.encodeDurationToMs(scanWait));
      }

      boolean positionalPresent = positionalParameters != null && !positionalParameters.isEmpty();
      if (positionalPresent) {
        try {
          JsonNode jsonArray = InternalJacksonSerDes.convertValue(
            JsonArray.from(positionalParameters), // ensures internal serializer can safely handle the values
            JsonNode.class
          );
          query.set("args", jsonArray);
        } catch (Exception e) {
          throw new IllegalArgumentException("Unsupported parameter type.", e);
        }
      }

      boolean namedParametersPresent = namedParameters != null && !namedParameters.isEmpty();
      if (namedParametersPresent) {
        try {
          // ignore result; just ensure internal serializer can safely handle the values
          JsonObject.from(namedParameters);

          namedParameters.forEach((key, value) -> {
            JsonNode jsonValue = toRepackagedJacksonNode(InternalJacksonSerDes.INSTANCE, value);
            if (key.charAt(0) != '$') {
              query.set('$' + key, jsonValue);
            } else {
              query.set(key, jsonValue);
            }
          });
        } catch (Exception e) {
          throw new IllegalArgumentException("Unsupported parameter type.", e);
        }
      }

      if (readOnly != null) {
        query.put("readonly", readOnly);
      }

      if (raw != null) {
        // ignore result; just ensure internal serializer can safely handle the values
        JsonObject.from(raw);

        raw.forEach((key, value) -> {
          JsonNode jsonValue = toRepackagedJacksonNode(InternalJacksonSerDes.INSTANCE, value);
          query.set(key, jsonValue);
        });
      }
    }

    @Override
    public String toString() {
      return "QueryOptions{" +
        "timeout=" + timeout +
        ", maxRetries=" + maxRetries +
        ", clientContextId='" + clientContextId + '\'' +
        ", namedParameters=" + namedParameters +
        ", positionalParameters=" + positionalParameters +
        ", scanConsistency=" + scanConsistency +
        ", scanWait=" + scanWait +
        ", deserializer=" + deserializer +
        ", readOnly=" + readOnly +
        ", raw=" + raw +
        '}';
    }

    /**
     * Converts the given value to JSON using the given serializer,
     * then parses the JSON into a {@link JsonNode}.
     * <p>
     * This allows the user to specify query parameters as POJOs
     * and have them serialized using their chosen serializer.
     */
    private static JsonNode toRepackagedJacksonNode(
      JsonSerializer serializer,
      Object value
    ) {
      byte[] jsonArrayBytes = serializer.serialize(value);
      return Mapper.readTree(jsonArrayBytes);
    }

    public boolean readOnly() {
      return readOnly != null && readOnly;
    }

    public Map<String, Object> clientContext() {
      return emptyMap();
    }

    public @Nullable Duration timeout() {
      return timeout;
    }

    public @Nullable String clientContextId() {
      return clientContextId;
    }

    public @Nullable Map<String, ?> namedParameters() {
      return namedParameters;
    }

    public @Nullable List<?> positionalParameters() {
      return positionalParameters;
    }

    public @Nullable ScanConsistency scanConsistency() {
      return scanConsistency;
    }

    public @Nullable Duration scanWait() {
      return scanWait;
    }

    public @Nullable Deserializer deserializer() {
      return deserializer;
    }

    public @Nullable Integer maxRetries() {
      return maxRetries;
    }
  }
}
