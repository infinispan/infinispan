package org.infinispan.stats.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.manager.ClusterExecutor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.security.actions.SecurityActions;
import org.infinispan.stats.ClusterContainerStats;
import org.infinispan.stats.ContainerStats;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * @author Ryan Emerson
 * @since 9.0
 */
@Scope(Scopes.GLOBAL)
@MBean(objectName = ClusterContainerStats.OBJECT_NAME, description = "General container statistics aggregated across the cluster.")
public class ClusterContainerStatsImpl extends AbstractClusterStats implements ClusterContainerStats {

   private static final Log log = LogFactory.getLog(ClusterContainerStatsImpl.class);

   private ClusterExecutor clusterExecutor;
   private EmbeddedCacheManager cacheManager;

   public ClusterContainerStatsImpl() {
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
   void updateStats() throws Exception {
      List<Map<String, Number>> memoryMap = statistics();
      for (String attr: LONG_ATTRIBUTES) {
         putLongAttributes(memoryMap, attr);
      }
   }

   private List<Map<String, Number>> statistics() throws Exception {
      final List<Map<String, Number>> successfulResponseMaps = Collections.synchronizedList(new ArrayList<>());
      // protect against stats collection before the component is ready
      if (clusterExecutor != null) {
         CompletableFutures.await(clusterExecutor.submitConsumer(ignore -> ContainerStats.getLocalStatMaps(), (addr, stats, t) -> {
            if (t == null) {
               successfulResponseMaps.add(stats);
            }
         }));
      }
      return successfulResponseMaps;
   }

   @ManagedAttribute(description = "The maximum amount of free memory in bytes across the cluster JVMs",
         displayName = "Cluster wide available memory.",
         clusterWide = true)
   @Override
   public long getMemoryAvailable() {
      return getStatAsLong(MEMORY_AVAILABLE);
   }

   @ManagedAttribute(description = "The maximum amount of memory that JVMs across the cluster will attempt to utilise in bytes",
         displayName = "Cluster wide max memory of JVMs",
         clusterWide = true)
   @Override
   public long getMemoryMax() {
      return getStatAsLong(MEMORY_MAX);
   }

   @ManagedAttribute(description = "The total amount of memory in the JVMs across the cluster in bytes",
         displayName = "Cluster wide total memory",
         clusterWide = true)
   @Override
   public long getMemoryTotal() {
      return getStatAsLong(MEMORY_TOTAL);
   }

   @ManagedAttribute(description = "The amount of memory used by JVMs across the cluster in bytes",
         displayName = "Cluster wide memory utilisation",
         clusterWide = true)
   @Override
   public long getMemoryUsed() {
      return getStatAsLong(MEMORY_USED);
   }
}
