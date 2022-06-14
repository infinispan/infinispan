package org.infinispan.stats.impl;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.stats.ClusterContainerStats;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Provide statistics of the local JVM instance. When the statistics collection is disabled, we return -1.
 *
 * @author José Bolina
 * @since 14.0
 */
@Scope(Scopes.GLOBAL)
@MBean(objectName = LocalContainerStatsImpl.LOCAL_CONTAINER_STATS, description = "General statistic of local container.")
public class LocalContainerStatsImpl extends AbstractContainerStats implements ClusterContainerStats {
   public static final String LOCAL_CONTAINER_STATS = "LocalContainerStats";
   private static final Log log = LogFactory.getLog(LocalContainerStatsImpl.class);

   LocalContainerStatsImpl() {
      super(log);
   }

   @Inject
   public void init(GlobalConfiguration configuration) {
      this.statisticsEnabled = configuration.statistics();
   }

   @Override
   protected List<Map<String, Number>> statistics() throws Exception {
      return Collections.singletonList(getLocalStatMaps());
   }

   @ManagedAttribute(description = "The maximum amount of free memory in bytes in local JVM",
         displayName = "Local available memory.")
   @Override
   public long getMemoryAvailable() {
      return getStatAsLong(MEMORY_AVAILABLE);
   }

   @ManagedAttribute(description = "The maximum amount of memory in local JVM will attempt to utilise in bytes",
         displayName = "Local JVM max memory")
   @Override
   public long getMemoryMax() {
      return getStatAsLong(MEMORY_MAX);
   }

   @ManagedAttribute(description = "The total amount of memory in the local JVM in bytes",
         displayName = "Local total memory")
   @Override
   public long getMemoryTotal() {
      return getStatAsLong(MEMORY_TOTAL);
   }

   @ManagedAttribute(description = "The amount of memory used by the local JVM in bytes",
         displayName = "Local memory utilisation")
   @Override
   public long getMemoryUsed() {
      return getStatAsLong(MEMORY_USED);
   }
}
