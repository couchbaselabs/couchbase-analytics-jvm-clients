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
package com.couchbase.analytics.fit.performer.util;

import com.couchbase.analytics.client.java.AnalyticsException;
import com.couchbase.analytics.client.java.AnalyticsTimeoutException;
import com.couchbase.analytics.client.java.InvalidCredentialException;
import com.couchbase.analytics.client.java.QueryException;
import com.couchbase.analytics.client.java.internal.utils.lang.CbThrowables;
import fit.columnar.PlatformErrorType;


public class ErrorUtil {
  private ErrorUtil() {
    throw new AssertionError("not instantiable");
  }

  private static fit.columnar.PlatformErrorType convertPlatformError(Throwable exception) {
    return (exception instanceof IllegalArgumentException)
      ? PlatformErrorType.PLATFORM_ERROR_INVALID_ARGUMENT
      : PlatformErrorType.PLATFORM_ERROR_OTHER;
  }

  public static fit.columnar.Error convertError(Throwable raw) {
    var ret = fit.columnar.Error.newBuilder();

    if (raw instanceof AnalyticsException) {
      var out = fit.columnar.ColumnarError.newBuilder()
        .setAsString(CbThrowables.getStackTraceAsString(raw));

      if (raw instanceof QueryException queryException) {
        out.setSubException(fit.columnar.SubColumnarError.newBuilder().setQueryException(
          fit.columnar.QueryException.newBuilder()
            .setErrorCode(queryException.code())
            .setServerMessage(queryException.serverMessage())
            .build())
          .build());
      }
      if (raw instanceof InvalidCredentialException) {
        out.setSubException(fit.columnar.SubColumnarError.newBuilder().setInvalidCredentialException(
          fit.columnar.InvalidCredentialException.newBuilder().build())
          .build());
      }

      if (raw instanceof AnalyticsTimeoutException) {
        out.setSubException(fit.columnar.SubColumnarError.newBuilder().setTimeoutException(fit.columnar.TimeoutException.newBuilder().build())
          .build());
      }

      if (raw.getCause() != null) {
        out.setCause(convertError(raw.getCause()));
      }

      ret.setColumnar(out);
    } else {
      ret.setPlatform(fit.columnar.PlatformError.newBuilder()
        .setType(convertPlatformError(raw))
        .setAsString(raw.toString()));
    }

    return ret.build();
  }
}
