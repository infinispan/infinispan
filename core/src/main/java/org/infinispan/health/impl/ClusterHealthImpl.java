package org.infinispan.health.impl;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.health.CacheHealth;
import org.infinispan.health.ClusterHealth;
import org.infinispan.health.HealthStatus;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.registry.InternalCacheRegistry;

class ClusterHealthImpl implements ClusterHealth {

   private final EmbeddedCacheManager cacheManager;
   private final InternalCacheRegistry internalCacheRegistry;
   private GlobalComponentRegistry gcr;

   ClusterHealthImpl(EmbeddedCacheManager cacheManager, InternalCacheRegistry internalCacheRegistry) {
      this.cacheManager = cacheManager;
      this.internalCacheRegistry = internalCacheRegistry;
      gcr = SecurityActions.getGlobalComponentRegistry(cacheManager);
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
         if (!cacheManager.cacheExists(cacheName))
            return null;

         ComponentRegistry cr = gcr.getNamedComponentRegistry(cacheName);
         if (cr == null)
            return new InvalidCacheHealth(cacheName);

         return new CacheHealthImpl(cr);
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


   @Override
   public Json toJson() {
      return Json.object()
            .set("cluster_name", getClusterName())
            .set("health_status", getHealthStatus())
            .set("number_of_nodes", getNumberOfNodes())
            .set("node_names", Json.make(getNodeNames()));
   }
}
