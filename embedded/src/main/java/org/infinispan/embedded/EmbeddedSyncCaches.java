package org.infinispan.embedded;

import org.infinispan.Cache;
import org.infinispan.api.configuration.CacheConfiguration;
import org.infinispan.api.sync.SyncCache;
import org.infinispan.api.sync.SyncCaches;
import org.infinispan.configuration.cache.Configuration;

/**
 * @since 15.0
 */
public class EmbeddedSyncCaches implements SyncCaches {
   private final Embedded embedded;

   EmbeddedSyncCaches(Embedded embedded) {
      this.embedded = embedded;
   }

   @Override
   public <K, V> SyncCache<K, V> get(String name) {
      Cache<K, V> cache = embedded.cacheManager.getCache(name);
      return new EmbeddedSyncCache<>(embedded, cache.getAdvancedCache());
   }

   @Override
   public <K, V> SyncCache<K, V> create(String name, CacheConfiguration cacheConfiguration) {
      Cache<K, V> cache = embedded.cacheManager.administration().getOrCreateCache(name, (Configuration) cacheConfiguration);
      return new EmbeddedSyncCache<>(embedded, cache.getAdvancedCache());
   }

   @Override
   public <K, V> SyncCache<K, V> create(String name, String template) {
      Cache<K, V> cache = embedded.cacheManager.administration().getOrCreateCache(name, template);
      return new EmbeddedSyncCache<>(embedded, cache.getAdvancedCache());
   }

   @Override
   public void remove(String name) {
      embedded.cacheManager.administration().removeCache(name);
   }

   @Override
   public Iterable<String> names() {
      return embedded.cacheManager.getCacheNames();
   }

   @Override
   public void createTemplate(String name, CacheConfiguration cacheConfiguration) {
      embedded.cacheManager.administration().createTemplate(name, (Configuration) cacheConfiguration);
   }

   @Override
   public void removeTemplate(String name) {
      embedded.cacheManager.administration().removeTemplate(name);
   }

   @Override
   public Iterable<String> templateNames() {
      return null;
   }
}
