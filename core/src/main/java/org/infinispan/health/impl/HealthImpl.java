package org.infinispan.health.impl;

import java.util.List;
import java.util.stream.Collectors;

import org.infinispan.health.CacheHealth;
import org.infinispan.health.ClusterHealth;
import org.infinispan.health.Health;
import org.infinispan.health.HostInfo;
import org.infinispan.manager.EmbeddedCacheManager;

public class HealthImpl implements Health {

    private final EmbeddedCacheManager embeddedCacheManager;

    public HealthImpl(EmbeddedCacheManager embeddedCacheManager) {
        this.embeddedCacheManager = embeddedCacheManager;
    }

    @Override
    public ClusterHealth getClusterHealth() {
        return new ClusterHealthImpl(embeddedCacheManager);
    }

    @Override
    public List<CacheHealth> getCacheHealth() {
        return embeddedCacheManager.getCacheNames().stream()
                .map(cacheName -> new CacheHealthImpl(embeddedCacheManager.getCache(cacheName)))
                .collect(Collectors.toList());
    }

    @Override
    public HostInfo getHostInfo() {
        return new HostInfoImpl();
    }
}
