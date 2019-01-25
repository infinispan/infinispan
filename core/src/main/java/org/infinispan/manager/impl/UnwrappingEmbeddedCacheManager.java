package org.infinispan.manager.impl;

import org.infinispan.Cache;
import org.infinispan.manager.EmbeddedCacheManager;

/**
 * EmbeddedCacheManager used for cluster executor invocation so that caches are not wrapped with security
 * since invoking via ClusterExecutor already requires ADMIN privileges
 * @author wburns
 * @since 10.0
 */
class UnwrappingEmbeddedCacheManager extends AbstractDelegatingEmbeddedCacheManager {
   public UnwrappingEmbeddedCacheManager(EmbeddedCacheManager cm) {
      super(cm);
   }

   @Override
   public <K, V> Cache<K, V> getCache() {
      Cache<K, V> cache = super.getCache();
      return SecurityActions.getUnwrappedCache(cache);
   }

   @Override
   public <K, V> Cache<K, V> getCache(String cacheName) {
      Cache<K, V> cache = super.getCache(cacheName);
      return SecurityActions.getUnwrappedCache(cache);
   }

   @Override
   public <K, V> Cache<K, V> getCache(String cacheName, boolean createIfAbsent) {
      Cache<K, V> cache = super.getCache(cacheName, createIfAbsent);
      return SecurityActions.getUnwrappedCache(cache);
   }

   @Override
   public <K, V> Cache<K, V> getCache(String cacheName, String configurationName) {
      Cache<K, V> cache = super.getCache(cacheName, configurationName);
      return SecurityActions.getUnwrappedCache(cache);
   }

   @Override
   public <K, V> Cache<K, V> getCache(String cacheName, String configurationTemplate, boolean createIfAbsent) {
      Cache<K, V> cache = super.getCache(cacheName, configurationTemplate, createIfAbsent);
      return SecurityActions.getUnwrappedCache(cache);
   }
}
