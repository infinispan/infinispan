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

public class ClusterHealthImpl implements ClusterHealth {

   private final EmbeddedCacheManager cacheManager;
   private final InternalCacheRegistry internalCacheRegistry;

   public ClusterHealthImpl(EmbeddedCacheManager cacheManager) {
      this.cacheManager = cacheManager;
      internalCacheRegistry = cacheManager.getGlobalComponentRegistry().getComponent(InternalCacheRegistry.class);
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

      if (healthStatuses.contains(HealthStatus.UNHEALTHY)) {
         globalHealthStatus = HealthStatus.UNHEALTHY;
      } else if (healthStatuses.contains(HealthStatus.REBALANCING)) {
         globalHealthStatus = HealthStatus.REBALANCING;
      }
      return globalHealthStatus;
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
