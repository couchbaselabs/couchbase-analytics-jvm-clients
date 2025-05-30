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

import com.couchbase.analytics.client.java.DataConversionException;
import com.couchbase.analytics.client.java.codec.Deserializer;
import com.couchbase.analytics.client.java.codec.JacksonDeserializer;
import com.couchbase.analytics.client.java.codec.TypeRef;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.json.JsonMapper;

import java.io.IOException;

/**
 * For internal SDK use only.
 *
 * @implNote The serializer is backed by a repackaged version of Jackson,
 * but this is an implementation detail users should not depend on.
 * <p>
 * Be aware that this serializer does not recognize standard Jackson annotations.
 * @see JacksonDeserializer
 */
public class InternalJacksonSerDes implements JsonSerializer, Deserializer {

  public static final InternalJacksonSerDes INSTANCE = new InternalJacksonSerDes();

  private static final JsonMapper mapper = JsonMapper.builder()
    .addModule(new JsonValueModule())
    .build();

  private InternalJacksonSerDes() {
  }

  public static <T> T convertValue(Object fromValue, Class<T> toType) {
    return mapper.convertValue(fromValue, toType);
  }

  @Override
  public byte[] serialize(final Object input) {
    try {
      return mapper.writeValueAsBytes(input);
    } catch (Exception e) {
      throw new DataConversionException(e);
    }
  }

  @Override
  public <T> T deserialize(final Class<T> target, final byte[] input) throws IOException {
    return mapper.readValue(input, target);
  }

  @Override
  public <T> T deserialize(final TypeRef<T> target, final byte[] input) throws IOException {
    JavaType type = mapper.getTypeFactory().constructType(target.type());
    return mapper.readValue(input, type);
  }
}
