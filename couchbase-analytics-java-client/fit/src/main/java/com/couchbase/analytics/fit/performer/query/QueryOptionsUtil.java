/*
 * Copyright (c) 2024 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.analytics.fit.performer.query;

import com.couchbase.analytics.client.java.QueryOptions;
import com.couchbase.analytics.client.java.ScanConsistency;
import com.couchbase.analytics.fit.performer.util.CustomDeserializer;
import com.couchbase.analytics.fit.performer.util.grpc.ProtobufConversions;
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.util.function.Consumer;

import static com.couchbase.analytics.fit.performer.util.grpc.ProtobufConversions.protobufStructToMap;

public class QueryOptionsUtil {
  public static @Nullable Consumer<QueryOptions> convertQueryOptions(fit.columnar.ExecuteQueryRequest executeQueryRequest) {
    if (!executeQueryRequest.hasOptions()) {
      return null;
    }

    return options -> {
      var opts = executeQueryRequest.getOptions();
      if (opts.hasPriority() && opts.getPriority()) {
        throw new UnsupportedOperationException("Specifying high priority query is not supported.");
      }
      if (opts.hasParametersPositional()) {
        options.parameters(ProtobufConversions.protobufListValueToList(opts.getParametersPositional()));
      }
      if (opts.hasParametersNamed()) {
        options.parameters(protobufStructToMap(opts.getParametersNamed()));
      }
      if (opts.hasReadonly()) {
        options.readOnly(opts.getReadonly());
      }
      if (opts.hasScanConsistency()) {
        options.scanConsistency(switch (opts.getScanConsistency()) {
          case SCAN_CONSISTENCY_REQUEST_PLUS -> ScanConsistency.REQUEST_PLUS;
          case SCAN_CONSISTENCY_NOT_BOUNDED -> ScanConsistency.NOT_BOUNDED;
          case UNRECOGNIZED -> throw new IllegalArgumentException("Bad scan consistency");
        });
      }
      if (opts.hasRaw()) {
        options.raw(protobufStructToMap(opts.getRaw()));
      }
      if (opts.hasTimeout()) {
        options.timeout(Duration.ofSeconds(opts.getTimeout().getSeconds()));
      }
      if (opts.hasDeserializer() && opts.getDeserializer().hasCustom()) {
        CustomDeserializer customDeserializer = new CustomDeserializer();
        options.deserializer(customDeserializer);
      }
      if (opts.hasMaxRetries()) {
        options.maxRetries(opts.getMaxRetries());
      }
    };
  }
}
