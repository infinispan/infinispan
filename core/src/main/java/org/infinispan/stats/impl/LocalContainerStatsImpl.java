package org.infinispan.stats.impl;

import static org.infinispan.stats.ContainerStats.LONG_ATTRIBUTES;
import static org.infinispan.stats.ContainerStats.MEMORY_AVAILABLE;
import static org.infinispan.stats.ContainerStats.MEMORY_MAX;
import static org.infinispan.stats.ContainerStats.MEMORY_TOTAL;
import static org.infinispan.stats.ContainerStats.MEMORY_USED;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.jmx.annotations.DataType;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.Units;
import org.infinispan.stats.ContainerStats;
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
public class LocalContainerStatsImpl extends AbstractStats implements ContainerStats {
   public static final String LOCAL_CONTAINER_STATS = "LocalContainerStats";
   private static final Log log = LogFactory.getLog(LocalContainerStatsImpl.class);

   public LocalContainerStatsImpl() {
      super(log);
   }

   @Inject
   public void init(GlobalConfiguration configuration) {
      this.statisticsEnabled = configuration.statistics();
   }
   @Override
   void updateStats() {
      List<Map<String, Number>> memoryMap = Collections.singletonList(ContainerStats.getLocalStatMaps());
      for (String attr: LONG_ATTRIBUTES) {
         putLongAttributes(memoryMap, attr);
      }
   }

   @ManagedAttribute(description = "The maximum amount of free memory in bytes in local JVM",
         displayName = "Local available memory.")
   public long getMemoryAvailable() {
      return getStatAsLong(MEMORY_AVAILABLE);
   }

   @ManagedAttribute(description = "The maximum amount of memory in local JVM will attempt to utilise in bytes",
         displayName = "Local JVM max memory")
   public long getMemoryMax() {
      return getStatAsLong(MEMORY_MAX);
   }

   @ManagedAttribute(description = "The total amount of memory in the local JVM in bytes",
         displayName = "Local total memory")
   public long getMemoryTotal() {
      return getStatAsLong(MEMORY_TOTAL);
   }

   @ManagedAttribute(description = "The amount of memory used by the local JVM in bytes",
         displayName = "Local memory utilisation")
   public long getMemoryUsed() {
      return getStatAsLong(MEMORY_USED);
   }

   @ManagedAttribute(description = "Gets the threshold for cluster wide stats refresh (milliseconds)",
         displayName = "Stale Stats Threshold",
         dataType = DataType.TRAIT,
         writable = true)
   public long getStaleStatsThreshold() {
      return staleStatsThreshold;
   }

   @ManagedAttribute(
         description = "Number of seconds since the statistics were last reset",
         displayName = "Seconds since statistics were reset",
         units = Units.SECONDS
   )
   public long getTimeSinceReset() {
      long result = -1;
      if (isStatisticsEnabled()) {
         result = timeService.timeDuration(resetNanoseconds.get(), TimeUnit.SECONDS);
      }
      return result;
   }

   @ManagedAttribute(description = "Enables or disables the gathering of statistics by this component",
         displayName = "Statistics enabled",
         dataType = DataType.TRAIT,
         writable = true)
   public boolean isStatisticsEnabled() {
      return getStatisticsEnabled();
   }
}
