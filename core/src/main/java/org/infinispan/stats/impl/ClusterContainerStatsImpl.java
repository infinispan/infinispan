package org.infinispan.stats.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.manager.ClusterExecutor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.stats.ClusterContainerStats;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * @author Ryan Emerson
 * @since 9.0
 */
@Scope(Scopes.GLOBAL)
@MBean(objectName = ClusterContainerStats.OBJECT_NAME, description = "General container statistics aggregated across the cluster.")
public class ClusterContainerStatsImpl extends AbstractContainerStats implements ClusterContainerStats {

   private static final Log log = LogFactory.getLog(ClusterContainerStatsImpl.class);

   private ClusterExecutor clusterExecutor;
   private EmbeddedCacheManager cacheManager;

   ClusterContainerStatsImpl() {
      super(log);
   }

   @Inject
   public void init(EmbeddedCacheManager cacheManager, GlobalConfiguration configuration) {
      this.cacheManager = cacheManager;
      this.statisticsEnabled = configuration.statistics();
   }

   @Override
   public void start() {
      this.clusterExecutor = SecurityActions.getClusterExecutor(cacheManager);
   }

   @Override
   protected List<Map<String, Number>> statistics() throws Exception {
      final List<Map<String, Number>> successfulResponseMaps = new ArrayList<>();
      CompletableFutures.await(clusterExecutor.submitConsumer(ignore -> getLocalStatMaps(), (addr, stats, t) -> {
         if (t == null) {
            successfulResponseMaps.add(stats);
         }
      }));
      return successfulResponseMaps;
   }

   @ManagedAttribute(description = "The maximum amount of free memory in bytes across the cluster JVMs",
         displayName = "Cluster wide available memory.")
   @Override
   public long getMemoryAvailable() {
      return getStatAsLong(MEMORY_AVAILABLE);
   }

   @ManagedAttribute(description = "The maximum amount of memory that JVMs across the cluster will attempt to utilise in bytes",
         displayName = "Cluster wide max memory of JVMs")
   @Override
   public long getMemoryMax() {
      return getStatAsLong(MEMORY_MAX);
   }

   @ManagedAttribute(description = "The total amount of memory in the JVMs across the cluster in bytes",
         displayName = "Cluster wide total memory")
   @Override
   public long getMemoryTotal() {
      return getStatAsLong(MEMORY_TOTAL);
   }

   @ManagedAttribute(description = "The amount of memory used by JVMs across the cluster in bytes",
         displayName = "Cluster wide memory utilisation")
   @Override
   public long getMemoryUsed() {
      return getStatAsLong(MEMORY_USED);
   }
}
