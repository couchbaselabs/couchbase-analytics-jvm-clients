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

import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

@ThreadSafe
public final class Scope implements Queryable {
  private final Cluster cluster;
  private final Database database;
  private final QueryContext queryContext;

  private final String name;

  Scope(
    Cluster cluster,
    Database database,
    String name
  ) {
    this.cluster = requireNonNull(cluster);
    this.database = requireNonNull(database);
    this.name = requireNonNull(name);
    this.queryContext = new QueryContext(database.name(), name);
  }

  /**
   * Returns the database this scope belongs to.
   */
  public Database database() {
    return database;
  }

  public String name() {
    return name;
  }

  @Override
  public QueryResult executeQuery(String statement, Consumer<QueryOptions> options) {
    return cluster.queryExecutor.executeQuery(queryContext, statement, options);
  }

  @Override
  public QueryMetadata executeStreamingQuery(
    String statement,
    Consumer<Row> rowAction,
    Consumer<QueryOptions> options
  ) {
    return cluster.queryExecutor.executeStreamingQueryWithRetry(queryContext, statement, rowAction, options);
  }

  @Override
  public String toString() {
    return "Scope{" +
      "name='" + name + '\'' +
      ", databaseName='" + database.name() + '\'' +
      '}';
  }
}
