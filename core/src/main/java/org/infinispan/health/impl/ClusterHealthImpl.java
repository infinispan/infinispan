package org.infinispan.health.impl;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.health.CacheHealth;
import org.infinispan.health.ClusterHealth;
import org.infinispan.health.HealthStatus;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.registry.InternalCacheRegistry;

class ClusterHealthImpl implements ClusterHealth {

   private final EmbeddedCacheManager cacheManager;
   private final InternalCacheRegistry internalCacheRegistry;

   ClusterHealthImpl(EmbeddedCacheManager cacheManager, InternalCacheRegistry internalCacheRegistry) {
      this.cacheManager = cacheManager;
      this.internalCacheRegistry = internalCacheRegistry;
   }

   @Override
   public HealthStatus getHealthStatus() {
      return Stream.concat(cacheManager.getCacheNames().stream(), internalCacheRegistry.getInternalCacheNames().stream())
            .map(this::getCacheHealth)
            .filter(Objects::nonNull)
            .map(CacheHealth::getStatus)
            .filter(h -> !h.equals(HealthStatus.HEALTHY))
            .findFirst().orElse(HealthStatus.HEALTHY);
   }

   private CacheHealth getCacheHealth(String cacheName) {
      try {
         Cache<?, ?> cache = cacheManager.getCache(cacheName, false);
         return cache != null ? new CacheHealthImpl(cache) : null;
      } catch (CacheException cacheException) {
         return new InvalidCacheHealth(cacheName);
      }
   }

   @Override
   public String getClusterName() {
      return cacheManager.getClusterName();
   }

   @Override
   public int getNumberOfNodes() {
      return Optional.ofNullable(cacheManager.getMembers()).orElse(Collections.emptyList())
                     .size();
   }

   @Override
   public List<String> getNodeNames() {
      return Optional.ofNullable(cacheManager.getMembers()).orElse(Collections.emptyList())
                     .stream()
                     .map(Object::toString)
                     .collect(Collectors.toList());
   }
}
