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

package com.couchbase.analytics.fit.performer.rpc;

import com.couchbase.analytics.client.java.Scope;
import com.couchbase.analytics.fit.performer.cluster.AnalyticsClusterConnection;
import fit.columnar.ExecutionContextClusterLevel;
import fit.columnar.ExecutionContextScopeLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

// Anything that needs to be shared between the services.
public class IntraServiceContext {
  private final static Logger logger = LoggerFactory.getLogger(IntraServiceContext.class);
  private final Map<String, AnalyticsClusterConnection> clusters = new HashMap<>();

  public void addCluster(String clusterConnectionId, AnalyticsClusterConnection cc) {
    synchronized (clusters) {
      clusters.put(clusterConnectionId, cc);
    }
  }

  public Map<String, AnalyticsClusterConnection> clusters() {
    synchronized (clusters) {
      return new HashMap<>(clusters);
    }
  }

  public void closeAndRemoveCluster(String clusterConnectionId) {
    synchronized (clusters) {
      clusters.get(clusterConnectionId).close();
      clusters.remove(clusterConnectionId);
    }
  }

  public void closeAndRemoveAllClusters() {
    synchronized (clusters) {
      logger.info("Closing {} clusters", clusters.size());
      clusters.values().forEach(AnalyticsClusterConnection::close);
      clusters.clear();
    }

  }

  public AnalyticsClusterConnection cluster(ExecutionContextClusterLevel executionContext) {
    synchronized (clusters) {
      return clusters.get(executionContext.getClusterId());
    }
  }

  public AnalyticsClusterConnection cluster(String clusterId) {
    synchronized (clusters) {
      return clusters.get(clusterId);
    }
  }

  public Scope scope(ExecutionContextScopeLevel executionContext) {
    synchronized (clusters) {
      return clusters.get(executionContext.getClusterId())
        .cluster()
        .database(executionContext.getDatabaseName())
        .scope(executionContext.getScopeName());
    }
  }
}
