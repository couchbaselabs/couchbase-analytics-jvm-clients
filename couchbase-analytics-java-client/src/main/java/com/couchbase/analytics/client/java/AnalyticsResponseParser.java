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

import com.couchbase.jsonskiff.JsonStreamParser;
import org.jspecify.annotations.Nullable;

import java.io.Closeable;
import java.io.InputStream;
import java.util.List;
import java.util.function.Consumer;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

class AnalyticsResponseParser implements Closeable {

  private final Consumer<byte[]> rowConsumer;

  private final JsonStreamParser parser;

  public AnalyticsResponseParser(Consumer<byte[]> rowConsumer) {
    this.rowConsumer = requireNonNull(rowConsumer);
    this.parser = newStreamParser();
  }

  @Nullable String requestId;
  byte @Nullable [] signature;
  byte @Nullable [] plans;
  byte @Nullable [] metrics;
  byte @Nullable [] errors;
  byte @Nullable [] warnings;
  @Nullable String clientContextId;
  @Nullable String status;

  private JsonStreamParser newStreamParser() {
    return JsonStreamParser.builder()
      .doOnValue("/requestID", v -> requestId = v.readString())
      .doOnValue("/signature", v -> signature = v.bytes())
      .doOnValue("/plans", v -> plans = v.bytes())
      .doOnValue("/clientContextID", v -> clientContextId = v.readString())
      .doOnValue("/results/-", v -> rowConsumer.accept(v.bytes()))
      .doOnValue("/status", v -> status = v.readString())
      .doOnValue("/metrics", v -> metrics = v.bytes())
      .doOnValue("/warnings", v -> warnings = v.bytes())
      .doOnValue("/errors", v -> fail(errors = v.bytes()))
      .build();
  }

  public void feed(InputStream is) {
    parser.feed(is, 4096);
  }

  public void feed(byte[] bytes, int offset, int len) {
    parser.feed(bytes, offset, len);
  }

  public void endOfInput() {
    parser.endOfInput();
  }

  private void fail(byte[] errors) {
    List<ErrorCodeAndMessage> parsed = ErrorCodeAndMessage.fromJsonArray(errors);

    ErrorCodeAndMessage primary = parsed.stream()
      .filter(it -> !it.retry())
      .findFirst()
      .orElse(parsed.get(0));

    QueryException e = new QueryException(primary);

    for (ErrorCodeAndMessage error : parsed) {
      if (error != primary) {
        e.addSuppressed(new QueryException(error));
      }
    }

    throw e;
  }

  public void close() {
    parser.close();
  }

  static @Nullable String newString(byte @Nullable [] array) {
    return array == null ? null : new String(array, UTF_8);
  }

  @Override
  public String toString() {
    return "AnalyticsResponseParser{" +
      "requestId='" + requestId + '\'' +
      ", signature=" + newString(signature) +
      ", plans=" + newString(plans) +
      ", metrics=" + newString(metrics) +
      ", errors=" + newString(errors) +
      ", warnings=" + newString(warnings) +
      ", clientContextId='" + clientContextId + '\'' +
      ", status='" + status + '\'' +
      '}';
  }
}
