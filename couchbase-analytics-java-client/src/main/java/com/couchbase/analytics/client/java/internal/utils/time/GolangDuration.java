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

package com.couchbase.analytics.client.java.internal.utils.time;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Helper methods that have to do with certain golang-specific format the server uses.
 */
public class GolangDuration {
  private GolangDuration() {
  }

  private static final Pattern durationComponentPattern = Pattern.compile("(?:([\\d.]+)([^\\d.]*))");
  private static final Pattern durationPattern = Pattern.compile(durationComponentPattern.pattern() + "+");

  /**
   * Encodes a Java duration into the encoded golang format.
   *
   * @param duration the duration to encode.
   * @return the encoded duration.
   */
  public static String encodeDurationToMs(final Duration duration) {
    return duration.toMillis() + "ms";
  }

  /**
   * Parses a Go duration string using the same rules as Go's
   * <a href="https://golang.org/pkg/time/#ParseDuration">time.ParseDuration</a> method.
   * <p>
   * A Go duration string is a possibly signed sequence of decimal numbers,
   * each with optional fraction and a unit suffix, such as "300ms", "-1.5h" or "2h45m".
   * Valid time units are "ns", "us" (or "µs"), "ms", "s", "m", "h".
   * The unit suffix may be omitted if the duration is "0".
   * <p>
   * The internal representation is a signed long number of nanoseconds.
   * This means the longest possible duration is approximately 290 years.
   *
   * @throws IllegalArgumentException if the input does not match the Go duration string syntax
   * or if it describes a duration longer than 2^63−1 nanoseconds.
   */
  public static Duration parseDuration(final String duration) {
    final boolean negative = duration.startsWith("-");
    final String abs = negative || duration.startsWith("+") ? duration.substring(1) : duration;

    if (abs.equals("0")) {
      return Duration.ZERO;
    }

    try {
      final Matcher validator = durationPattern.matcher(abs);
      if (!validator.matches()) {
        throw new IllegalArgumentException("Invalid duration.");
      }

      long resultNanos = 0;

      final Matcher m = durationComponentPattern.matcher(abs);
      while (m.find()) {
        final BigDecimal number = new BigDecimal(m.group(1));
        final TimeUnit timeUnit = parseTimeUnit(m.group(2));
        resultNanos = Math.addExact(resultNanos, toNanosExact(number, timeUnit));
      }

      return Duration.ofNanos(negative ? -resultNanos : resultNanos);

    } catch (ArithmeticException e) {
      throw new IllegalArgumentException("Duration \"" + duration + "\" is too long. Maximum duration is 2^63−1 nanoseconds.", e);

    } catch (Exception e) {
      final String msg = " ; A duration string is a possibly signed sequence of decimal numbers," +
        " each with optional fraction and a unit suffix, such as \"300ms\", \"-1.5h\" or \"2h45m\"." +
        " Valid time units are \"ns\", \"us\" (or \"µs\"), \"ms\", \"s\", \"m\", \"h\"";

      throw new IllegalArgumentException(
        "Failed to parse duration \"" + duration + "\" ; " + e + msg, e);
    }
  }

  private static long toNanosExact(BigDecimal number, TimeUnit timeUnit) {
    return number.multiply(BigDecimal.valueOf(timeUnit.toNanos(1)))
      .setScale(0, RoundingMode.DOWN) // drop any fractional nanoseconds
      .longValueExact(); // throw ArithmeticException if greater than Long.MAX_VALUE
  }

  @SuppressWarnings("UnnecessaryUnicodeEscape")
  private static TimeUnit parseTimeUnit(String unit) {
    switch (unit) {
      case "":
        throw new IllegalArgumentException("Missing time unit.");
      case "h":
        return TimeUnit.HOURS;
      case "m":
        return TimeUnit.MINUTES;
      case "s":
        return TimeUnit.SECONDS;
      case "ms":
        return TimeUnit.MILLISECONDS;
      case "us":
      case "\u00B5s": // micro symbol
      case "\u03BCs": // Greek letter mu
        return TimeUnit.MICROSECONDS;
      case "ns":
        return TimeUnit.NANOSECONDS;
      default:
        throw new IllegalArgumentException("Unknown unit \"" + unit + "\".");
    }
  }

}
