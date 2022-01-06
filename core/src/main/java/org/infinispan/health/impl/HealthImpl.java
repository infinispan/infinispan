package org.infinispan.health.impl;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.infinispan.commons.CacheException;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
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
      return embeddedCacheManager.getCacheNames().stream().map(this::getHealth).collect(Collectors.toList());
   }

   private CacheHealth getHealth(String cacheName) {
      try {
         GlobalComponentRegistry gcr = SecurityActions.getGlobalComponentRegistry(embeddedCacheManager);
         ComponentRegistry cr = gcr.getNamedComponentRegistry(cacheName);
         if (cr == null)
            return new InvalidCacheHealth(cacheName);

         return new CacheHealthImpl(cr);
      } catch (CacheException cacheException) {
         return new InvalidCacheHealth(cacheName);
      }
   }

   @Override
   public List<CacheHealth> getCacheHealth(Set<String> cacheNames) {
      return cacheNames.stream().map(this::getHealth).collect(Collectors.toList());
   }

   @Override
   public HostInfo getHostInfo() {
      return hostInfoImpl;
   }
}
