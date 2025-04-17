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

import org.jspecify.annotations.Nullable;

import static com.couchbase.analytics.client.java.internal.utils.lang.CbStrings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

/**
 * Thrown when the Analytics cluster returns an error message in response to a query request.
 */
public final class QueryException extends AnalyticsException {
  final ErrorCodeAndMessage errorCodeAndMessage;

  QueryException(ErrorCodeAndMessage errorCodeAndMessage) {
    this(errorCodeAndMessage, null);
  }

  QueryException(
    ErrorCodeAndMessage errorCodeAndMessage,
    @Nullable String ctx
  ) {
    super(errorCodeAndMessage + (isNullOrEmpty(ctx) ? "" : (" ; " + ctx)));
    this.errorCodeAndMessage = requireNonNull(errorCodeAndMessage);
  }

  /**
   * Returns the Analytics error code sent by the server.
   */
  // todo include a documentation reference link in the Javadoc
  public int code() {
    return errorCodeAndMessage.code();
  }

  /**
   * Returns the human-readable error message sent by the server, without
   * the additional context returned by {@link #getMessage()}.
   * <p>
   * <b>Caveat:</b> The content and structure of this message may change
   * between Couchbase Server versions.
   */
  public String serverMessage() {
    return errorCodeAndMessage.message();
  }
}
