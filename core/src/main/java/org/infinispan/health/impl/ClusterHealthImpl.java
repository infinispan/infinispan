package org.infinispan.health.impl;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
      HealthStatus globalHealthStatus = HealthStatus.HEALTHY;

      Set<HealthStatus> healthStatuses = Stream.concat(cacheManager.getCacheNames().stream(), internalCacheRegistry.getInternalCacheNames().stream())
            .map(cacheName -> cacheManager.getCache(cacheName, false))
            .filter(Objects::nonNull)
            .map(CacheHealthImpl::new)
            .map(CacheHealthImpl::getStatus)
            .collect(Collectors.toSet());

      if (healthStatuses.contains(HealthStatus.DEGRADED)) {
         globalHealthStatus = HealthStatus.DEGRADED;
      } else if (healthStatuses.contains(HealthStatus.HEALTHY_REBALANCING)) {
         globalHealthStatus = HealthStatus.HEALTHY_REBALANCING;
      }
      return globalHealthStatus;
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
