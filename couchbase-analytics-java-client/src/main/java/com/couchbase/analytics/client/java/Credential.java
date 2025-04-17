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
import okhttp3.Credentials;
import okhttp3.tls.HandshakeCertificates;

import java.util.function.Supplier;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

/**
 * Create an instance like this:
 * <pre>
 * Credential.of(username, password)
 * </pre>
 * <p>
 * For advanced use cases involving dynamic credentials, see
 * {@link Credential#ofDynamic(Supplier)}.
 */
@ThreadSafe
public abstract class Credential {

  /**
   * Returns a new instance that holds the given username and password.
   */
  public static Credential of(String username, String password) {
    String authHeaderValue = Credentials.basic(username, password, UTF_8);

    return new Credential() {
      @Override
      String httpAuthorizationHeaderValue() {
        return authHeaderValue;
      }

      @Override
      void addHeldCertificate(HandshakeCertificates.Builder builder) {
        // noop
      }
    };
  }

  /**
   * Returns a new instance of a dynamic credential that invokes the given supplier
   * every time a credential is required.
   * <p>
   * This enables updating a credential without having to restart your application.
   */
  public static Credential ofDynamic(Supplier<Credential> supplier) {
    requireNonNull(supplier);

    return new Credential() {
      @Override
      String httpAuthorizationHeaderValue() {
        return supplier.get().httpAuthorizationHeaderValue();
      }

      @Override
      void addHeldCertificate(HandshakeCertificates.Builder builder) {
        supplier.get().addHeldCertificate(builder);
      }
    };
  }

  abstract String httpAuthorizationHeaderValue();

  abstract void addHeldCertificate(HandshakeCertificates.Builder builder);

  /**
   * @see #of
   * @see #ofDynamic
   */
  private Credential() {
  }
}
