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

import com.couchbase.analytics.client.java.internal.ThreadSafe;
import com.couchbase.analytics.client.java.internal.utils.json.Mapper;
import com.couchbase.analytics.client.java.internal.utils.time.GolangDuration;
import com.fasterxml.jackson.databind.JsonNode;

import java.time.Duration;
import java.util.Optional;

/**
 * Holds the metrics as returned from an analytics response.
 */
@ThreadSafe
public final class QueryMetrics {

  /**
   * Provides a pointer into the root nodes of the raw response for easier decoding.
   */
  private final JsonNode rootNode;

  /**
   * Creates new {@link QueryMetrics} from the raw data.
   *
   * @param raw the raw analytics data.
   */
  QueryMetrics(final byte[] raw) {
    try {
      this.rootNode = Mapper.readTree(raw);
    } catch (Exception e) {
      throw new DataConversionException("Could not parse analytics metrics!", e);
    }
  }

  /**
   * @return The total time taken for the request that is the time from when the
   * request was received until the results were returned, in a human-readable
   * format (eg. 123.45ms for a little over 123 milliseconds).
   */
  public Duration elapsedTime() {
    return decode(String.class, "elapsedTime").map(GolangDuration::parseDuration).orElse(Duration.ZERO);
  }

  /**
   * @return The time taken for the execution of the request, that is the time from
   * when query execution started until the results were returned.
   */
  public Duration executionTime() {
    return decode(String.class, "executionTime").map(GolangDuration::parseDuration).orElse(Duration.ZERO);
  }

  /**
   * @return The total number of objects in the results.
   */
  public long resultCount() {
    return decode(Long.class, "resultCount").orElse(0L);
  }

  /**
   * @return The total number of bytes in the results.
   */
  public long resultSize() {
    return decode(Long.class, "resultSize").orElse(0L);
  }

  /**
   * @return The number of processed objects for the request.
   */
  public long processedObjects() {
    return decode(Long.class, "processedObjects").orElse(0L);
  }

  /**
   * Helper method to turn a given path of the raw data into the target class.
   *
   * @param target the target class to decode into.
   * @param path the path of the raw json.
   * @param <T> the generic type to decide into.
   * @return the generic decoded object if present and not null.
   */
  private <T> Optional<T> decode(final Class<T> target, final String path) {
    try {
      JsonNode subNode = rootNode.path(path);
      if (subNode == null || subNode.isNull() || subNode.isMissingNode()) {
        return Optional.empty();
      }
      return Optional.of(Mapper.convertValue(subNode, target));
    } catch (Exception e) {
      throw new DataConversionException("Could not decode " + path + " in analytics metrics!", e);
    }
  }

  @Override
  public String toString() {
    return "QueryMetrics{" +
      "raw=" + rootNode +
      '}';
  }

}
