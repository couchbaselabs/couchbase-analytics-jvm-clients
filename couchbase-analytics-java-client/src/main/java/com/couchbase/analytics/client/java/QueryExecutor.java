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
import com.couchbase.analytics.client.java.internal.utils.UserAgentBuilder;
import com.couchbase.analytics.client.java.internal.utils.VersionAndGitHash;
import com.couchbase.analytics.client.java.internal.utils.json.Mapper;
import com.couchbase.analytics.client.java.internal.utils.time.Deadline;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import okhttp3.Call;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import okio.BufferedSink;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLHandshakeException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.UncheckedIOException;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.function.Consumer;

import static com.couchbase.analytics.client.java.internal.utils.lang.CbObjects.defaultIfNull;
import static com.couchbase.analytics.client.java.internal.utils.lang.CbThrowables.hasCause;
import static com.couchbase.analytics.client.java.internal.utils.time.GolangDuration.encodeDurationToMs;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

class QueryExecutor {
  private static final Logger log = LoggerFactory.getLogger(QueryExecutor.class);

  private static final BackoffCalculator backoff = new BackoffCalculator(
    Duration.ofMillis(100),
    Duration.ofMinutes(1)
  );

  private final VersionAndGitHash versionAndGitHash = VersionAndGitHash.from(getClass());

  private final String userAgent = new UserAgentBuilder()
    .append(
      "java-analytics",
      versionAndGitHash.version(),
      "id=" + UUID.randomUUID()
    )
    .appendJava()
    .appendOs()
    .build();

  private final AnalyticsOkHttpClient httpClient;
  private final HttpUrl url;
  private final Credential credential;
  private final Deserializer defaultDeserializer;
  private final ClusterOptions.Unmodifiable clusterOptions;
  private final boolean maybeCouchbaseInternalNonProd;

  public QueryExecutor(
    AnalyticsOkHttpClient httpClient,
    HttpUrl url,
    Credential credential,
    ClusterOptions.Unmodifiable clusterOptions
  ) {
    this.httpClient = requireNonNull(httpClient);
    this.url = requireNonNull(url);
    this.credential = requireNonNull(credential);
    this.clusterOptions = requireNonNull(clusterOptions);

    this.defaultDeserializer = requireNonNull(clusterOptions.deserializer());
    this.maybeCouchbaseInternalNonProd = url.host().endsWith(".nonprod-project-avengers.com");
  }

  public QueryResult executeQuery(@Nullable QueryContext queryContext, String statement, Consumer<QueryOptions> options) {
    List<Row> rows = new ArrayList<>();
    QueryMetadata metadata = executeStreamingQueryWithRetry(queryContext, statement, rows::add, options);
    return new QueryResult(rows, metadata);
  }

  /**
   * This exception's cause is an exception throw by the user's row action callback,
   * which we want to rethrow verbatim.
   * <p>
   * Exists to differentiate a user exception from a JSON stream parser failure
   * which we might want to transform into some other exception.
   */
  private static final class RowActionException extends RuntimeException {
    public RowActionException(Throwable cause) {
      super(cause);
    }
  }

  public QueryMetadata executeStreamingQueryWithRetry(
    @Nullable QueryContext queryContext,
    String statement,
    Consumer<Row> rowAction,
    Consumer<QueryOptions> options
  ) {
    QueryException prevException = null;

    QueryOptions queryOptions = new QueryOptions();
    options.accept(queryOptions);
    QueryOptions.Unmodifiable opts = queryOptions.build();

    Deserializer deserializer = defaultIfNull(opts.deserializer(), defaultDeserializer);
    Duration timeout = defaultIfNull(opts.timeout(), clusterOptions.timeout().queryTimeout());

    Deadline retryDeadline = Deadline.of(timeout);

    try {
      for (int attempt = 0; ; attempt++) {
        try {
          return executeStreamingQueryOnce(queryContext, statement, rowAction, opts, deserializer, timeout);

        } catch (QueryException e) {
          if (!e.errorCodeAndMessage.retry()) {
            throw e;
          }

          prevException = e;

          Duration backoffDelay = backoff.delayForAttempt(attempt);
          if (!retryDeadline.hasRemaining(backoffDelay)) {
            AnalyticsTimeoutException timeoutException = new AnalyticsTimeoutException(
              "Declaring timeout early because sleeping for backoff delay would exceed timeout deadline."
            );
            throw timeoutException;
          }

          log.debug("Query execution failed with error code {}; will retry in {}", e.errorCodeAndMessage, backoffDelay);
          sleep(backoffDelay);

          timeout = retryDeadline.remaining().orElseThrow(() ->
            new AnalyticsTimeoutException("Query execution did not complete within timeout")
          );
        }
      }

    } catch (RuntimeException e) {
      if (prevException != null) {
        e.addSuppressed(prevException);
      }
      throw e;
    }
  }

  private static void sleep(Duration d) {
    try {
      MILLISECONDS.sleep(d.toMillis());
    } catch (InterruptedException e) {
      throw propagateInterruption(e);
    }
  }

  QueryMetadata executeStreamingQueryOnce(
    @Nullable QueryContext queryContext,
    String statement,
    Consumer<Row> rowAction,
    QueryOptions.Unmodifiable opts,
    Deserializer deserializer,
    Duration timeout
  ) {
    requireNonNull(statement, "statement cannot be null");

    Duration serverTimeout = timeout.plus(Duration.ofSeconds(5));

    ObjectNode query = JsonNodeFactory.instance.objectNode()
      .put("statement", statement)
      .put("timeout", encodeDurationToMs(serverTimeout));

    if (queryContext != null) {
      query.put("query_context", queryContext.format());
    }

    opts.injectParams(query);

    Request.Builder requestBuilder = new Request.Builder()
      .url(url)
      .header("User-Agent", userAgent)
      .header("Authorization", credential.httpAuthorizationHeaderValue())
      .post(requestBody(query));

    Request request = requestBuilder.build();

    OkHttpClient client = httpClient.clientWithTimeout(timeout);
    Call call = client.newCall(request);

    boolean allowConnectionReuse = false;

    try (
      AnalyticsResponseParser parser = new AnalyticsResponseParser(rowBytes -> {
        try {
          rowAction.accept(new Row(rowBytes, deserializer));
        } catch (RuntimeException e) {
          // Wrap to distinguish from other parse exceptions.
          throw new RowActionException(e);
        }
      });
      Response response = AnalyticsOkHttpClient.executeInterruptibly(call);
      ResponseBody body = response.body()
    ) {
      String httpStatusMessage = "HTTP response status: " + statusLine(response);

      if (response.code() == 401) {
        throw new InvalidCredentialException(httpStatusMessage);
      }

      if (body == null) {
        maybeThrowSyntheticServiceNotAvailable(response);
        throw new AnalyticsException("HTTP response had no body; this is unexpected! " + httpStatusMessage);
      }

      try (InputStream is = body.byteStream()) {
        parser.feed(is);
        parser.endOfInput();
        allowConnectionReuse = true; // success!

      } catch (RowActionException e) {
        // Catch separately in case the user's row action threw UncheckedIOException!
        allowConnectionReuse = true; // the failure wasn't due to a degraded node or dead connection.
        throw (RuntimeException) e.getCause();

      } catch (UncheckedIOException e) {
        // Thread was interrupted, or got a non-blank non-JSON response.
        if (Thread.currentThread().isInterrupted()) {
          throw propagateInterruption(e);
        }
        throw new AnalyticsException("Query response parsing failed due to " + e + " ; " + httpStatusMessage, e);
      }

      if (parser.requestId == null) {
        maybeThrowSyntheticServiceNotAvailable(response);

        // Response wasn't a JSON Object with a "requestID" field :-(
        throw new AnalyticsException("HTTP body did not matched expected query response format. " + httpStatusMessage);
      }

      return new QueryMetadata(parser);

    } catch (SSLHandshakeException e) {
      throw new AnalyticsException(tlsHandshakeErrorMessage(e), e);

    } catch (IOException e) {
      if (Thread.currentThread().isInterrupted()) {
        throw propagateInterruption(e);
      }

      if (hasCause(e, SocketTimeoutException.class) || hasCause(e, InterruptedIOException.class)) {
        throw new AnalyticsTimeoutException(e);
      }

      throw new AnalyticsException(e);

    } finally {
      if (!allowConnectionReuse) {
        preventConnectionReuse();
      }
    }
  }

  /**
   * In some deployments, we might get HTTP status code 503 (Service Unavailable)
   * from an intermediary like a proxy or load balancer. In that case, throw a
   * {@link QueryException} with the same error code as if the response
   * had come from the Analytics cluster. This has two benefits:
   * <ul>
   * <li> It engages the SDK's automatic retry mechanism, since the synthetic
   * exception is marked as retriable.
   *
   * <li> It lets the user handle "service unavailable" the same way, regardless of
   * whether the error came from a load balancer or the analytics server.
   * </ul>
   */
  private static void maybeThrowSyntheticServiceNotAvailable(Response response) {
    if (response.code() == 503) {
      int analyticsServiceNotAvailable = 23000;
      String message = "Got HTTP status " + statusLine(response) + " -- but there was no analytics response body. This might indicate the HTTP response came from a proxy or load balancer.";
      boolean retriable = true;
      throw new QueryException(new ErrorCodeAndMessage(analyticsServiceNotAvailable, message, retriable, emptyMap()));
    }
  }

  private void preventConnectionReuse() {
    // Can't mark just one connection as not reusable, so...
    log.debug("Clearing connection pool to avoid potentially reusing a connection to a degraded node.");
    httpClient.evictAll();
  }

  private String tlsHandshakeErrorMessage(Throwable tlsHandshakeError) {
    String message = "A TLS handshake problem prevented the client from connecting to the server." +
      " Potential causes include the server (or a proxy, or an on-path attacker)" +
      " presenting a certificate the client is not configured to trust." +
      " If connecting to a hosted service, make sure to use a relatively recent" +
      " SDK version that has up-to-date certificates." +
      " If connecting from inside a corporate network, make sure to configure the SDK" +
      " to trust the CA certificate of your proxy." +
      " Error message from the TLS engine: " + tlsHandshakeError;

    if (maybeCouchbaseInternalNonProd) {
      message = "It looks like you might be trying to connect to a Couchbase internal non-production hosted service." +
        " If this is true, please make sure you have configured the SDK to trust the non-prod certificate authority, like this:" +
        "\n\n" +
        "Cluster cluster = Cluster.newInstance(\n" +
        "  connectionString,\n" +
        "  Credential.of(username, password),\n" +
        "  clusterOptions -> clusterOptions\n" +
        "    .security(it -> it.trustOnlyCertificates(Certificates.getNonProdCertificates()))\n" +
        ");\n\n" +
        "We now return to your regularly scheduled exception message.\n\n"
        + message;
    }

    return message;
  }


  public static CancellationException propagateInterruption(Throwable cause) {
    Thread.currentThread().interrupt();
    CancellationException e = new CancellationException("Thread was interrupted.");
    e.initCause(cause);
    return e;
  }

  private static String statusLine(Response response) {
    return response.code() + " " + response.message();
  }

  private static RequestBody requestBody(JsonNode json) {
    return new JsonNodeRequestBody(json);
  }

  public void close() {
    httpClient.close();
  }

  static class JsonNodeRequestBody extends RequestBody {
    private static final MediaType APPLICATION_JSON = requireNonNull(MediaType.parse("application/json"));

    private final JsonNode json;

    public JsonNodeRequestBody(JsonNode json) {
      this.json = requireNonNull(json);
    }

    @Override
    public MediaType contentType() {
      return APPLICATION_JSON;
    }

    @Override
    public void writeTo(BufferedSink sink) {
      Mapper.writeValue(sink.outputStream(), json);
    }

    @Override
    public String toString() {
      return json.toString();
    }
  }
}
