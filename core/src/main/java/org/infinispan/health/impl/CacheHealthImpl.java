package org.infinispan.health.impl;

import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.health.CacheHealth;
import org.infinispan.health.HealthStatus;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.partitionhandling.AvailabilityMode;
import org.infinispan.partitionhandling.impl.PartitionHandlingManager;

public class CacheHealthImpl implements CacheHealth {

   private final ComponentRegistry cr;

   public CacheHealthImpl(ComponentRegistry cr) {
      this.cr = cr;
   }

   @Override
   public String getCacheName() {
      return cr.getCacheName();
   }

   @Override
   public HealthStatus getStatus() {
      if (cr.getStatus() == ComponentStatus.INITIALIZING) return HealthStatus.INITIALIZING;

      PartitionHandlingManager partitionHandlingManager = cr.getComponent(PartitionHandlingManager.class);
      if (!isComponentHealthy() || partitionHandlingManager.getAvailabilityMode() == AvailabilityMode.DEGRADED_MODE) {
         return HealthStatus.DEGRADED;
      }
      DistributionManager distributionManager = cr.getDistributionManager();
      if (distributionManager != null && distributionManager.isRehashInProgress()) {
         return HealthStatus.HEALTHY_REBALANCING;
      }
      return HealthStatus.HEALTHY;
   }

   private boolean isComponentHealthy() {
      switch (cr.getStatus()) {
         case INSTANTIATED:
         case RUNNING:
            return true;
         default:
            return false;
      }
   }

}
