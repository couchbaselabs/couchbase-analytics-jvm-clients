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

import com.couchbase.analytics.client.java.codec.JacksonDeserializer;
import com.fasterxml.jackson.databind.json.JsonMapper;

/**
 * Initialization-on-demand holder for the default deserializer.
 */
public final class DefaultJacksonDeserializerHolder {
  public static final JacksonDeserializer DESERIALIZER = new JacksonDeserializer(
    JsonMapper.builder().build()
  );

  private DefaultJacksonDeserializerHolder() {
  }
}
