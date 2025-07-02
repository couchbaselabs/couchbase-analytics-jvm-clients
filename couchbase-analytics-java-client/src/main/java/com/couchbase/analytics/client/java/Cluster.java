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

import com.couchbase.analytics.client.java.internal.Certificates;
import com.couchbase.analytics.client.java.internal.utils.BuilderPropertySetter;
import okhttp3.HttpUrl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.function.Consumer;

import static com.couchbase.analytics.client.java.internal.utils.lang.CbCollections.listOf;
import static com.couchbase.analytics.client.java.internal.utils.lang.CbCollections.setOf;

public class Cluster implements Queryable, Closeable {
  private static final Logger log = LoggerFactory.getLogger(Cluster.class);

  private static final BuilderPropertySetter propertySetter = BuilderPropertySetter.builder()
    .registerCommonTypes()
    .pathComponentTransformer(Cluster::lowerSnakeCaseToLowerCamelCase)
    .build();

  final QueryExecutor queryExecutor;


  private static HttpUrl parseAnalyticsUrl(String s) {
    HttpUrl url = HttpUrl.get(s);
    HttpUrl.Builder builder = url.newBuilder();

    if (!url.username().isEmpty() || !url.password().isEmpty()) {
      throw new IllegalArgumentException("Connection string must not have username or password");
    }

    if (url.pathSegments().equals(listOf(""))) {
      builder
//        .addPathSegment("analytics")
//        .addPathSegment("service");
        .addPathSegment("api")
        .addPathSegment("v1")
        .addPathSegment("request");

    } else {
      throw new IllegalArgumentException("Connection string must not have path components.");
    }

//    if (URI.create(s).getPort() == -1) {
//      builder.port(url.isHttps() ? 18095 : 8095);
//    }

    return builder.build();
  }

  private Cluster(String connectionString, Credential credential, ClusterOptions.Unmodifiable options) {
    HttpUrl url = parseAnalyticsUrl(connectionString);
    this.queryExecutor = new QueryExecutor(
      new AnalyticsOkHttpClient(options, url, credential),
      url,
      credential,
      options
    );

    warnIfConfigurationIsInsecure(url, options);
  }

  private static void warnIfConfigurationIsInsecure(
    HttpUrl url,
    ClusterOptions.Unmodifiable options
  ) {
    boolean insecure = false;

    if (!url.isHttps()) {
      log.warn("Insecure configuration: URL does not use `https` scheme.");
      insecure = true;

    } else if (options.security().trustSource().isInsecure()) {
      insecure = true;
      log.warn("Insecure configuration: Server certificate verification was explicitly disabled.");
    }

    if (insecure) {
      log.debug("Insecure configuration was created here:", new RuntimeException("Insecure configuration"));
    }
  }

  public static Cluster newInstance(String connectionString, Credential credential) {
    return newInstance(connectionString, credential, options -> {
    });
  }

  private static String withoutQueryParameters(String url) {
    int index = url.indexOf('?');
    return index == -1 ? url : url.substring(0, index);
  }

  public static Cluster newInstance(String connectionString, Credential credential, Consumer<ClusterOptions> options) {
    ClusterOptions opts = new ClusterOptions();
    options.accept(opts);
    applyConnectionStringParameters(opts, HttpUrl.get(connectionString));
    return new Cluster(withoutQueryParameters(connectionString), credential, opts.build());
  }

  private static LinkedHashMap<String, String> queryParameters(HttpUrl url) {
    LinkedHashMap<String, String> params = new LinkedHashMap<>();
    for (String name : url.queryParameterNames()) {
      List<String> values = url.queryParameterValues(name);
      params.put(name, values.get(values.size() - 1));
    }
    return params;
  }

  private static void applyConnectionStringParameters(ClusterOptions builder, HttpUrl url) {
    // Make a mutable copy so we can remove entries that require special handling.
    LinkedHashMap<String, String> params = queryParameters(url);

    // "security.trust_only_non_prod" is special; it doesn't have a corresponding programmatic
    // config option. It's not a secret, but we don't want to confuse external users with a
    // security config option they never need to set.
    boolean trustOnlyNonProdCertificates = lastTrustParamIsNonProd(params);

    propertySetter.set(builder, params);

    // Do this last, after any other "trust_only_*" params are validated and applied.
    // Otherwise, the earlier params would clobber the config set by this param.
    // (There's no compelling use case for including multiple "trust_only_*" params in
    // the connection string, but we behave consistently if someone tries it.)
    if (trustOnlyNonProdCertificates) {
      builder.security(it -> it.trustOnlyCertificates(Certificates.getNonProdCertificates()));
    }
  }

  private static boolean lastTrustParamIsNonProd(LinkedHashMap<String, String> params) {
    final String TRUST_ONLY_NON_PROD_PARAM = "security.trust_only_non_prod";

    // Last trust param wins, so check whether "trust only non-prod" was last trust param.
    boolean trustOnlyNonProdWasLast = params.keySet().stream()
      .filter(it -> it.startsWith("security.trust_"))
      .reduce((a, b) -> b) // last
      .orElse("")
      .equals(TRUST_ONLY_NON_PROD_PARAM);

    // Always remove it, so later processing doesn't treat it as unrecognized param.
    String trustOnlyNonProdValue = params.remove(TRUST_ONLY_NON_PROD_PARAM);

    // Always validate if present, regardless of whether it was last.
    if (trustOnlyNonProdValue != null && !setOf("", "true", "1").contains(trustOnlyNonProdValue)) {
      throw new IllegalArgumentException("Invalid value for connection string property '" + TRUST_ONLY_NON_PROD_PARAM + "'; expected 'true', '1', or empty string, but got: '" + trustOnlyNonProdValue + "'");
    }

    return trustOnlyNonProdWasLast;
  }

  private static String lowerSnakeCaseToLowerCamelCase(String s) {
    StringBuilder sb = new StringBuilder();
    int[] codePoints = s.codePoints().toArray();

    boolean prevWasUnderscore = false;
    for (int i : codePoints) {
      if (i == '_') {
        prevWasUnderscore = true;
        continue;
      }

      if (prevWasUnderscore) {
        i = Character.toUpperCase(i);
      }
      sb.appendCodePoint(i);
      prevWasUnderscore = false;
    }

    return sb.toString();
  }


  /**
   * Returns the database in this cluster with the given name.
   * <p>
   * A database is a container for {@link Scope}s.
   * <p>
   * If the database does not exist, this method still returns a
   * non-null object, but operations using that object fail with
   * an exception indicating the database does not exist.
   */
  public Database database(String name) {
    return new Database(this, name);
  }

  @Override
  public QueryResult executeQuery(String statement, Consumer<QueryOptions> options) {
    try {
      return queryExecutor.executeQuery(null, statement, options);

    } catch (QueryException e) {
      // Expected, so omit uninteresting noise from the JSON stream parser.
      e.fillInStackTrace();
      throw e;
    }
  }

  @Override
  public QueryMetadata executeStreamingQuery(String statement, Consumer<Row> rowAction, Consumer<QueryOptions> options) {
    try {
      return queryExecutor.executeStreamingQueryWithRetry(null, statement, rowAction, options);

    } catch (QueryException e) {
      // Expected, so omit uninteresting noise from the JSON stream parser.
      e.fillInStackTrace();
      throw e;
    }
  }

  @Override
  public void close() {
    queryExecutor.close();
  }
}
