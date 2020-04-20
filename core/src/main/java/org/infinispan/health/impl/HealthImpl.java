package org.infinispan.health.impl;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.infinispan.health.CacheHealth;
import org.infinispan.health.ClusterHealth;
import org.infinispan.health.Health;
import org.infinispan.health.HostInfo;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.registry.InternalCacheRegistry;

public class HealthImpl implements Health {

   private final EmbeddedCacheManager embeddedCacheManager;
   private final InternalCacheRegistry internalCacheRegistry;
   private final HostInfo hostInfoImpl = new HostInfoImpl();

   public HealthImpl(EmbeddedCacheManager embeddedCacheManager, InternalCacheRegistry internalCacheRegistry) {
      this.embeddedCacheManager = embeddedCacheManager;
      this.internalCacheRegistry = internalCacheRegistry;
   }

   @Override
   public ClusterHealth getClusterHealth() {
      return new ClusterHealthImpl(embeddedCacheManager, internalCacheRegistry);
   }

   @Override
   public List<CacheHealth> getCacheHealth() {
      return embeddedCacheManager.getCacheNames().stream()
            .map(cacheName -> new CacheHealthImpl(SecurityActions.getCache(embeddedCacheManager, cacheName)))
            .collect(Collectors.toList());
   }

   @Override
   public List<CacheHealth> getCacheHealth(Set<String> cacheNames) {
      return cacheNames.stream()
            .map(cacheName -> new CacheHealthImpl(SecurityActions.getCache(embeddedCacheManager, cacheName)))
            .collect(Collectors.toList());
   }

   @Override
   public HostInfo getHostInfo() {
      return hostInfoImpl;
   }
}
