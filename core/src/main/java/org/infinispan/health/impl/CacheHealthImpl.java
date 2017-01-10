package org.infinispan.health.impl;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.health.CacheHealth;
import org.infinispan.health.HealthStatus;
import org.infinispan.partitionhandling.AvailabilityMode;

public class CacheHealthImpl implements CacheHealth {

    private final AdvancedCache<?, ?> cache;

    public CacheHealthImpl(Cache<?, ?> cache) {
        this.cache = cache.getAdvancedCache();
    }

    @Override
    public String getCacheName() {
        return cache.getName();
    }

    @Override
    public HealthStatus getStatus() {
        if (!isComponentHealthy() || cache.getAvailability() == AvailabilityMode.DEGRADED_MODE) {
            return HealthStatus.UNHEALTHY;
        }
        DistributionManager distributionManager = cache.getDistributionManager();
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
