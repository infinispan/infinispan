package org.infinispan.health.impl;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.infinispan.health.ClusterHealth;
import org.infinispan.health.HealthStatus;
import org.infinispan.manager.EmbeddedCacheManager;

public class ClusterHealthImpl implements ClusterHealth {

    private final EmbeddedCacheManager cacheManager;

    public ClusterHealthImpl(EmbeddedCacheManager cacheManager) {
        this.cacheManager = cacheManager;
    }

    @Override
    public HealthStatus getHealthStatus() {
        Set<HealthStatus> healthStatuses = cacheManager.getCacheNames().stream()
                .map(cacheName -> cacheManager.getCache(cacheName))
                .map(cache -> new CacheHealthImpl(cache))
                .map(cacheHealth -> cacheHealth.getStatus())
                .collect(Collectors.toSet());

        if (healthStatuses.contains(HealthStatus.UNHEALTHY)) {
            return HealthStatus.UNHEALTHY;
        } else if (healthStatuses.contains(HealthStatus.REBALANCING)) {
            return HealthStatus.REBALANCING;
        }
        return HealthStatus.HEALTHY;
    }

    @Override
    public String getClusterName() {
        return cacheManager.getClusterName();
    }

    @Override
    public int getNumberOfNodes() {
        return Optional.ofNullable(cacheManager.getTransport()).map(t -> t.getMembers().size()).orElse(1);
    }

    @Override
    public List<String> getNodeNames() {
        return Optional.ofNullable(cacheManager.getTransport()).map(t -> t.getMembers()).orElse(Collections.emptyList())
              .stream()
              .map(member -> member.toString())
              .collect(Collectors.toList());
    }
}
