package org.infinispan.health.impl;

import org.infinispan.Cache;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.health.CacheHealth;
import org.infinispan.health.HealthStatus;
import org.infinispan.partitionhandling.AvailabilityMode;

public class CacheHealthImpl implements CacheHealth {

   private final Cache<?, ?> cache;

   public CacheHealthImpl(Cache<?, ?> cache) {
      this.cache = cache;
   }

   @Override
   public String getCacheName() {
      return cache.getName();
   }

   @Override
   public HealthStatus getStatus() {
      if (!isComponentHealthy() || cache.getAdvancedCache().getAvailability() == AvailabilityMode.DEGRADED_MODE) {
         return HealthStatus.UNHEALTHY;
      }
      DistributionManager distributionManager = SecurityActions.getDistributionManager(cache);
      if (distributionManager != null && distributionManager.isRehashInProgress()) {
         return HealthStatus.REBALANCING;
      }
      return HealthStatus.HEALTHY;
   }

   private boolean isComponentHealthy() {
      switch (cache.getStatus()) {
         case INSTANTIATED:
         case RUNNING:
            return true;
         default:
            return false;
      }
   }
}
