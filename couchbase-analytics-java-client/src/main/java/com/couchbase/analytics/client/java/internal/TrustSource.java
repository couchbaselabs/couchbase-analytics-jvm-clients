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

package com.couchbase.analytics.client.java.internal;


import com.couchbase.analytics.client.java.internal.utils.security.CbCertificates;
import org.jspecify.annotations.Nullable;

import javax.net.ssl.TrustManagerFactory;
import java.nio.file.Path;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

/**
 * Either a {@link TrustManagerFactory} XOR a list of {@link X509Certificate}.
 */
public class TrustSource {
  private static final TrustSource INSECURE = new TrustSource(null, new TrustManagerFactory(null, null, null) {});

  @Nullable private final List<X509Certificate> certificates;
  @Nullable private final TrustManagerFactory factory;

  private TrustSource(
    @Nullable List<X509Certificate> certificates,
    @Nullable TrustManagerFactory factory
  ) {
    if (countNonNull(certificates, factory) != 1) {
      throw new IllegalArgumentException("Must specify exactly one of 'certificates' or `factory'");
    }
    if (certificates != null && certificates.isEmpty()) {
      throw new IllegalArgumentException("When specifying a list of certificates, the list must not be empty.");
    }

    this.certificates = certificates;
    this.factory = factory;
  }

  /**
   * Returns a new instance that trusts the certificate authorities trusted by this Java runtime environment.
   */
  public static TrustSource fromJvm() {
    return from(CbCertificates.getJvmCertificates());
  }

  /**
   * Returns a new instance that trusts the certificate authorities trusted by this Java runtime environment,
   * plus the Capella production certificate.
   */
  public static TrustSource fromJvmAndCapella() {
    List<X509Certificate> trustedCertificates = new ArrayList<>(CbCertificates.getJvmCertificates());
    trustedCertificates.addAll(CbCertificates.getCapellaCaCertificates());
    return from(trustedCertificates);
  }

  /**
   * Returns a new instance that trusts the certificates in the specified PEM file.
   */
  public static TrustSource from(Path pemFilePath) {
    List<X509Certificate> certs = CbCertificates.read(pemFilePath);
    if (certs.isEmpty()) {
      throw new IllegalArgumentException("PEM file contained no suitable certificates: " + pemFilePath);
    }
    return from(certs);
  }

  /**
   * Returns a new instance that trusts the specified certificates.
   */
  public static TrustSource from(List<X509Certificate> certificates) {
    requireNonNull(certificates);
    if (certificates.isEmpty()) {
      throw new IllegalArgumentException("certificates list must not be empty");
    }

    return new TrustSource(certificates, null);
  }

  /**
   * Returns a new instance backed by the specified factory.
   */
  public static TrustSource from(TrustManagerFactory factory) {
    return new TrustSource(null, requireNonNull(factory));
  }

  /**
   * Returns a new instance that does not verify server certificates.
   */
  public static TrustSource insecure() {
    return INSECURE;
  }

  public @Nullable List<X509Certificate> certificates() {
    return certificates;
  }

  public @Nullable TrustManagerFactory trustManagerFactory() {
    return factory;
  }

  public boolean isInsecure() {
    return this == INSECURE;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("TrustSource{");
    if (certificates != null) {
      sb.append("certificates=").append(describe(certificates));
    } else {
      sb.append("factory=").append(factory);
    }
    sb.append("}");
    return sb.toString();

  }

  private static int countNonNull(@Nullable Object... objects) {
    return (int) Arrays.stream(objects).filter(Objects::nonNull).count();
  }

  private static @Nullable String describe(@Nullable List<X509Certificate> certs) {
    if (certs == null) {
      return null;
    }

    if (certs.size() <= 10) {
      return certs.stream()
        .map(TrustSource::describe)
        .collect(joining(", ", "[", "]"));
    }

    return "(" + certs.size() + " certificates)";
  }

  private static String describe(X509Certificate cert) {
    return cert.getSubjectX500Principal()
      + " (valid from " + cert.getNotBefore().toInstant() + " to " + cert.getNotAfter().toInstant() + ")";
  }

}
