package org.infinispan.query.impl;

import org.infinispan.AdvancedCache;
import org.infinispan.configuration.cache.ClusteringConfiguration;
import org.infinispan.partitionhandling.AvailabilityMode;
import org.infinispan.partitionhandling.PartitionHandling;
import org.infinispan.query.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * @since 9.4
 */
public final class PartitionHandlingSupport {

   private static final Log LOGGER = LogFactory.getLog(PartitionHandlingSupport.class, Log.class);

   private final boolean isClustered;
   private final PartitionHandling partitionHandling;
   private final AdvancedCache<?, ?> cache;

   public PartitionHandlingSupport(AdvancedCache<?, ?> cache) {
      this.cache = cache;
      ClusteringConfiguration clusteringConfiguration = cache.getCacheConfiguration().clustering();
      this.isClustered = clusteringConfiguration.cacheMode().isClustered();
      this.partitionHandling = isClustered ? clusteringConfiguration.partitionHandling().whenSplit() : null;
   }

   public void checkCacheAvailable() {
      if (!isClustered) return;

      AvailabilityMode availability = cache.getAvailability();
      if (availability == AvailabilityMode.AVAILABLE) return;

      if (partitionHandling != PartitionHandling.ALLOW_READ_WRITES) {
         throw LOGGER.partitionDegraded();
      }
   }

}
