/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2012, Red Hat, Inc., and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.jboss.as.clustering.infinispan.subsystem;

import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.NAME;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OP_ADDR;

import java.util.HashMap;
import java.util.Map;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.server.infinispan.spi.service.CacheServiceName;
import org.infinispan.stats.ClusterCacheStats;
import org.jboss.as.controller.AbstractRuntimeOnlyHandler;
import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.PathAddress;
import org.jboss.as.controller.SimpleAttributeDefinitionBuilder;
import org.jboss.as.controller.registry.ManagementResourceRegistration;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;
import org.jboss.msc.service.ServiceController;

/**
 * Handler which manages read-only access to clustered cache runtime information (metrics)
 *
 * @see ClusterCacheStats
 * @author Vladimir Blagojevic
 */
public class ClusteredCacheMetricsHandler extends AbstractRuntimeOnlyHandler {
   public static final ClusteredCacheMetricsHandler INSTANCE = new ClusteredCacheMetricsHandler();

   public enum ClusteredCacheMetrics {

      NUMBER_OF_LOCKS_AVAILABLE(ClusterWideMetricKeys.NUMBER_OF_LOCKS_AVAILABLE, ModelType.INT, true),
      NUMBER_OF_LOCKS_HELD(ClusterWideMetricKeys.NUMBER_OF_LOCKS_HELD, ModelType.INT, true),

      AVERAGE_READ_TIME(ClusterWideMetricKeys.AVERAGE_READ_TIME, ModelType.LONG, true),
      AVERAGE_WRITE_TIME(ClusterWideMetricKeys.AVERAGE_WRITE_TIME, ModelType.LONG, true),
      AVERAGE_REMOVE_TIME(ClusterWideMetricKeys.AVERAGE_REMOVE_TIME, ModelType.LONG, true),
      AVERAGE_READ_TIME_NANOS(ClusterWideMetricKeys.AVERAGE_READ_TIME_NANOS, ModelType.LONG, true),
      AVERAGE_WRITE_TIME_NANOS(ClusterWideMetricKeys.AVERAGE_WRITE_TIME_NANOS, ModelType.LONG, true),
      AVERAGE_REMOVE_TIME_NANOS(ClusterWideMetricKeys.AVERAGE_REMOVE_TIME_NANOS, ModelType.LONG, true),
      TIME_SINCE_START(ClusterWideMetricKeys.TIME_SINCE_START, ModelType.LONG, true),
      EVICTIONS(ClusterWideMetricKeys.EVICTIONS, ModelType.LONG, true),
      HIT_RATIO(ClusterWideMetricKeys.HIT_RATIO, ModelType.DOUBLE, true),
      HITS(ClusterWideMetricKeys.HITS, ModelType.LONG, true),
      MISSES(ClusterWideMetricKeys.MISSES, ModelType.LONG, true),
      NUMBER_OF_ENTRIES(ClusterWideMetricKeys.NUMBER_OF_ENTRIES, ModelType.INT, true),
      NUMBER_OF_ENTRIES_IN_MEMORY(ClusterWideMetricKeys.NUMBER_OF_ENTRIES_IN_MEMORY, ModelType.INT, true),
      OFF_HEAP_MEMORY_USED(ClusterWideMetricKeys.OFF_HEAP_MEMORY_USED, ModelType.LONG, true),
      MINIMUM_REQUIRED_NODES(ClusterWideMetricKeys.MINIMUM_REQUIRED_NODES, ModelType.INT, true),
      READ_WRITE_RATIO(ClusterWideMetricKeys.READ_WRITE_RATIO,ModelType.DOUBLE, true),
      REMOVE_HITS(ClusterWideMetricKeys.REMOVE_HITS, ModelType.LONG, true),
      REMOVE_MISSES(ClusterWideMetricKeys.REMOVE_MISSES, ModelType.LONG, true),
      STORES(ClusterWideMetricKeys.STORES, ModelType.LONG, true),
      TIME_SINCE_RESET(ClusterWideMetricKeys.TIME_SINCE_RESET, ModelType.LONG, true),

      INVALIDATIONS(ClusterWideMetricKeys.INVALIDATIONS, ModelType.LONG, true),
      PASSIVATIONS(ClusterWideMetricKeys.PASSIVATIONS, ModelType.STRING, true),

      ACTIVATIONS(ClusterWideMetricKeys.ACTIVATIONS, ModelType.STRING, true),
      CACHE_LOADER_LOADS(ClusterWideMetricKeys.CACHE_LOADER_LOADS, ModelType.LONG, true),
      CACHE_LOADER_MISSES(ClusterWideMetricKeys.CACHE_LOADER_MISSES, ModelType.LONG, true),
      CACHE_LOADER_STORES(ClusterWideMetricKeys.CACHE_LOADER_STORES, ModelType.LONG, true),

      STALE_STATS_THRESHOLD(ClusterWideMetricKeys.STALE_STATS_THRESHOLD, ModelType.LONG, true);

      private static final Map<String, ClusteredCacheMetrics> MAP = new HashMap<>();

      static {
         for (ClusteredCacheMetrics metric : ClusteredCacheMetrics.values()) {
            MAP.put(metric.toString(), metric);
         }
      }

      final AttributeDefinition definition;
      final boolean clustered;

      private ClusteredCacheMetrics(final AttributeDefinition definition, final boolean clustered) {
         this.definition = definition;
         this.clustered = clustered;
      }

      private ClusteredCacheMetrics(String attributeName, ModelType type, boolean allowNull) {
         this(new SimpleAttributeDefinitionBuilder(attributeName, type, allowNull).setStorageRuntime().build(), true);
      }

      @Override
      public final String toString() {
         return definition.getName();
      }

      public static ClusteredCacheMetrics getStat(final String stringForm) {
         return MAP.get(stringForm);
      }
   }

   /*
    * Two constraints need to be dealt with here: 1. There may be no started cache instance
    * available to interrogate. Because of lazy deployment, a cache instance is only started upon
    * deployment of an application which uses that cache instance. 2. The attribute name passed in
    * may not correspond to a defined metric
    *
    * Read-only attributes have no easy way to throw an exception without negatively impacting other
    * parts of the system. Therefore in such cases, as message will be logged and a ModelNode of
    * undefined will be returned.
    */
   @Override
   protected void executeRuntimeStep(OperationContext context, ModelNode operation) {
      final PathAddress address = PathAddress.pathAddress(operation.require(OP_ADDR));
      final String cacheContainerName = address.getElement(address.size() - 2).getValue();
      final String cacheName = address.getLastElement().getValue();
      final String attrName = operation.require(NAME).asString();
      final ServiceController<?> controller = context.getServiceRegistry(false).getService(
            CacheServiceName.CACHE.getServiceName(cacheContainerName, cacheName));
      Cache<?, ?> cache = (Cache<?, ?>) controller.getValue();
      ClusteredCacheMetrics metric = ClusteredCacheMetrics.getStat(attrName);
      ModelNode result = new ModelNode();

      if (metric == null) {
         context.getFailureDescription().set(String.format("Unknown metric %s", attrName));
      } else if (cache == null) {
         context.getFailureDescription().set(String.format("Unavailable cache %s", attrName));
      } else {
         AdvancedCache<?, ?> aCache = cache.getAdvancedCache();
         ClusterCacheStats clusterCacheStats = aCache.getComponentRegistry().getComponent(ClusterCacheStats.class);
         switch (metric) {
         case NUMBER_OF_LOCKS_AVAILABLE: {
            result.set(clusterCacheStats.getNumberOfLocksAvailable());
            break;
         }
         case NUMBER_OF_LOCKS_HELD: {
            result.set(clusterCacheStats.getNumberOfLocksHeld());
            break;
         }
            case TIME_SINCE_START: {
               result.set(clusterCacheStats.getTimeSinceStart());
               break;
            }
         case AVERAGE_READ_TIME: {
            result.set(clusterCacheStats.getAverageReadTime());
            break;
         }
         case AVERAGE_WRITE_TIME: {
            result.set(clusterCacheStats.getAverageWriteTime());
            break;
         }
         case AVERAGE_REMOVE_TIME: {
            result.set(clusterCacheStats.getAverageRemoveTime());
            break;
         }
         case AVERAGE_READ_TIME_NANOS: {
            result.set(clusterCacheStats.getAverageReadTimeNanos());
            break;
         }
         case AVERAGE_WRITE_TIME_NANOS: {
            result.set(clusterCacheStats.getAverageWriteTimeNanos());
            break;
         }
         case AVERAGE_REMOVE_TIME_NANOS: {
            result.set(clusterCacheStats.getAverageRemoveTimeNanos());
            break;
         }
         case EVICTIONS: {
            result.set(clusterCacheStats.getEvictions());
            break;
         }
         case HIT_RATIO: {
            result.set(clusterCacheStats.getHitRatio());
            break;
         }
         case HITS: {
            result.set(clusterCacheStats.getHits());
            break;
         }
         case MISSES: {
            result.set(clusterCacheStats.getMisses());
            break;
         }
         case NUMBER_OF_ENTRIES: {
            result.set(clusterCacheStats.getCurrentNumberOfEntries());
            break;
         }
         case NUMBER_OF_ENTRIES_IN_MEMORY: {
            result.set(clusterCacheStats.getCurrentNumberOfEntriesInMemory());
            break;
         }
         case OFF_HEAP_MEMORY_USED:
            result.set(clusterCacheStats.getOffHeapMemoryUsed());
            break;
         case MINIMUM_REQUIRED_NODES:
            result.set(clusterCacheStats.getRequiredMinimumNumberOfNodes());
            break;
         case READ_WRITE_RATIO: {
            result.set(clusterCacheStats.getReadWriteRatio());
            break;
         }
         case REMOVE_HITS: {
            result.set(clusterCacheStats.getRemoveHits());
            break;
         }
         case REMOVE_MISSES: {
            result.set(clusterCacheStats.getRemoveMisses());
            break;
         }
         case STORES: {
            result.set(clusterCacheStats.getStores());
            break;
         }
         case TIME_SINCE_RESET: {
            result.set(clusterCacheStats.getTimeSinceReset());
            break;
         }
         case INVALIDATIONS: {
            result.set(clusterCacheStats.getInvalidations());
            break;
         }
         case PASSIVATIONS: {
            result.set(clusterCacheStats.getPassivations());
            break;
         }
         case ACTIVATIONS: {
            result.set(clusterCacheStats.getActivations());
            break;
         }
         case CACHE_LOADER_LOADS: {
            result.set(clusterCacheStats.getCacheLoaderLoads());
            break;
         }
         case CACHE_LOADER_MISSES: {
            result.set(clusterCacheStats.getCacheLoaderMisses());
            break;
         }
         case CACHE_LOADER_STORES: {
            result.set(clusterCacheStats.getStoreWrites());
            break;
         }
         case STALE_STATS_THRESHOLD:
            result.set(clusterCacheStats.getStaleStatsThreshold());
            break;
         default: {
            context.getFailureDescription().set(String.format("Unknown metric %s", metric));
            break;
         }
         }
         context.getResult().set(result);
      }
      context.stepCompleted();
   }

   public void registerClusteredMetrics(ManagementResourceRegistration container) {
      for (ClusteredCacheMetrics metric : ClusteredCacheMetrics.values()) {
         if (metric.clustered) {
            container.registerMetric(metric.definition, this);
         }
      }
   }
}
