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

import com.couchbase.analytics.client.java.Row;
import com.couchbase.analytics.fit.performer.content.ContentAsUtil;
import com.couchbase.analytics.fit.performer.util.ErrorUtil;
import fit.columnar.QueryRowResponse;

import javax.annotation.Nullable;


public class QueryRowUtil {
  public record RowProcessingResult(@Nullable Throwable errorThrown, QueryRowResponse row) {
  }

  public static RowProcessingResult processRow(fit.columnar.ExecuteQueryRequest executeQueryRequest, Row row) {
    if (executeQueryRequest.hasContentAs()) {
      var content = ContentAsUtil.contentType(executeQueryRequest.getContentAs(), row);
      if (content.isSuccess()) {
        return new RowProcessingResult(null, QueryRowResponse.newBuilder()
          // todo metadata
//                .setMetadata()
          .setSuccess(QueryRowResponse.Result.newBuilder()
            .setRow(QueryRowResponse.Row.newBuilder()
              .setRowContent(content.value())))
          .build());
      } else {
        return new RowProcessingResult(content.exception(), QueryRowResponse.newBuilder()
          // todo metadata
//                .setMetadata()
          .setRowLevelFailure(ErrorUtil.convertError(content.exception()))
          .build());
      }
    } else {
      return new RowProcessingResult(null, QueryRowResponse.newBuilder()
        // todo metadata
//                .setMetadata()
        .setSuccess(QueryRowResponse.Result.getDefaultInstance()).build());
    }
  }
}
