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

package com.couchbase.analytics.fit.performer;

import com.couchbase.analytics.fit.performer.rpc.IntraServiceContext;
import com.couchbase.analytics.fit.performer.rpc.JavaAnalyticsCrossService;
import com.couchbase.analytics.fit.performer.rpc.JavaAnalyticsService;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class JavaAnalyticsPerformer {
  private static final Logger logger = LoggerFactory.getLogger(JavaAnalyticsPerformer.class);

  public static void main(String[] args) throws IOException, InterruptedException {
    int port = 8060;

    var sharedContext = new IntraServiceContext();

    Server server = ServerBuilder.forPort(port)
      .addService(new JavaAnalyticsService(sharedContext))
      .addService(new JavaAnalyticsCrossService(sharedContext))
      .build();

    server.start();
    logger.info("Server Started at {}", server.getPort());
    server.awaitTermination();
  }

}
