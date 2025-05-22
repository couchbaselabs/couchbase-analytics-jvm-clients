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

package com.example.couchbase.analytics;

import com.couchbase.analytics.client.java.Cluster;
import com.couchbase.analytics.client.java.ClusterOptions;
import com.couchbase.analytics.client.java.Credential;
import com.couchbase.analytics.client.java.QueryOptions;
import com.couchbase.analytics.client.java.QueryResult;
import com.couchbase.analytics.client.java.Queryable;
import com.couchbase.analytics.client.java.Row;
import com.couchbase.analytics.client.java.codec.Deserializer;
import com.couchbase.analytics.client.java.extension.reactor.ReactorQueryResult;
import com.couchbase.analytics.client.java.extension.reactor.ReactorQueryable;
import com.example.couchbase.analytics.reactor.ReactorExample;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.List;

public class Example {
  public static void main(String[] args) {
    var connectionString = "https://...";
    var username = "...";
    var password = "...";

    try (Cluster cluster = Cluster.newInstance(
      connectionString,
      Credential.of(username, password),
      // The third parameter is optional.
      // This example sets the default query timeout to 2 minutes.
      clusterOptions -> clusterOptions
        .timeout(it -> it.queryTimeout(Duration.ofMinutes(2)))
    )) {

      bufferedQueryExample(cluster);

      streamingQueryExample(cluster);

      dataBindingExample(cluster);

      nullRowExample(cluster);

      reactorQueryExample(cluster);
    }
  }

  /**
   * Executes a query, buffering all result rows in memory.
   */
  static void bufferedQueryExample(Queryable clusterOrScope) {
    QueryResult result = clusterOrScope.executeQuery(
      "select ?=1",
      options -> options
        .readOnly(true)
        .parameters(List.of(1))
    );
    result.rows().forEach(row -> System.out.println("Got row: " + row));
  }

  /**
   * Executes a query, processing rows one-by-one
   * as they arrive from server.
   */
  static void streamingQueryExample(Queryable clusterOrScope) {
    clusterOrScope.executeStreamingQuery(
      "select ?=1",
      row -> System.out.println("Got row: " + row),
      options -> options
        .readOnly(true)
        .parameters(List.of(1))
    );
  }

  /**
   * Converts a result row to a user-defined type using the default Jackson
   * {@link Deserializer}.
   *
   * @see ClusterOptions#deserializer(Deserializer)
   * @see QueryOptions#deserializer(Deserializer)
   */
  static void dataBindingExample(Queryable clusterOrScope) {
    record MyRowPojo(String greeting) {}

    QueryResult result = clusterOrScope.executeQuery(
      "SELECT 'hello world' AS greeting"
    );
    MyRowPojo resultRow = result.rows().getFirst().as(MyRowPojo.class);
    System.out.println(resultRow.greeting);
  }

  /**
   * Calls {@link Row#asNullable} because null is an expected result row value.
   */
  static void nullRowExample(Queryable clusterOrScope) {
    QueryResult result = clusterOrScope.executeQuery("SELECT RAW null");
    String nullableString = result.rows().getFirst().asNullable(String.class);
    System.out.println(nullableString);
  }

  /**
   * Executes a query using the optional Project Reactor extension library.
   * <p>
   * Requires adding {@code com.couchbase.client:couchbase-analytics-java-client-reactor} as a dependency of your project.
   * <p>
   * See {@link ReactorExample} for more examples.
   */
  static void reactorQueryExample(Queryable clusterOrScope) {
    var reactor = ReactorQueryable.from(clusterOrScope);

    Flux<Row> resultRows = reactor.executeQuery("SELECT 1")
      .flatMapMany(ReactorQueryResult::rows);

    System.out.println(resultRows.blockLast());
  }
}
