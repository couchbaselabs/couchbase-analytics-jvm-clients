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

import com.couchbase.analytics.client.java.internal.RawQueryMetadata;
import com.couchbase.jsonskiff.JsonStreamParser;

import java.io.Closeable;
import java.io.InputStream;
import java.util.List;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

class AnalyticsResponseParser implements Closeable {

  private final Consumer<byte[]> rowConsumer;

  private final JsonStreamParser parser;

  public AnalyticsResponseParser(Consumer<byte[]> rowConsumer) {
    this.rowConsumer = requireNonNull(rowConsumer);
    this.parser = newStreamParser();
  }

  final RawQueryMetadata result = new RawQueryMetadata();

  private JsonStreamParser newStreamParser() {
    return JsonStreamParser.builder()
      .doOnValue("/requestID", v -> result.requestId = v.readString())
      .doOnValue("/signature", v -> result.signature = v.bytes())
      .doOnValue("/plans", v -> result.plans = v.bytes())
      .doOnValue("/clientContextID", v -> result.clientContextId = v.readString())
      .doOnValue("/results/-", v -> rowConsumer.accept(v.bytes()))
      .doOnValue("/status", v -> result.status = v.readString())
      .doOnValue("/metrics", v -> result.metrics = v.bytes())
      .doOnValue("/warnings", v -> result.warnings = v.bytes())
      .doOnValue("/errors", v -> fail(result.errors = v.bytes()))
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

  @Override
  public String toString() {
    return result.toString();
  }
}
