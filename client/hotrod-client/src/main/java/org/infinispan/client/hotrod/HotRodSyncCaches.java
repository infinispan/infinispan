package org.infinispan.client.hotrod;

import org.infinispan.api.Experimental;
import org.infinispan.api.configuration.CacheConfiguration;
import org.infinispan.api.sync.SyncCache;
import org.infinispan.api.sync.SyncCaches;
import org.infinispan.client.hotrod.impl.InternalRemoteCache;

/**
 * @since 14.0
 **/
@Experimental
final class HotRodSyncCaches implements SyncCaches {
   private final HotRod hotrod;

   HotRodSyncCaches(HotRod hotrod) {
      this.hotrod = hotrod;
   }

   @Override
   public <K, V> SyncCache<K, V> get(String name) {
      InternalRemoteCache<K, V> cache = getCache(name);
      return new HotRodSyncCache<>(hotrod, cache);
   }

   @Override
   public <K, V> SyncCache<K, V> create(String name, CacheConfiguration cacheConfiguration) {
      hotrod.cacheManager.getConfiguration().addRemoteCache(name, builder -> builder.configuration(cacheConfiguration.toString()));
      return get(name);
   }

   @Override
   public <K, V> SyncCache<K, V> create(String name, String template) {
      hotrod.cacheManager.getConfiguration().addRemoteCache(name, builder -> builder.templateName(template));
      return get(name);
   }

   @Override
   public void remove(String name) {
      hotrod.cacheManager.administration().removeCache(name);
   }

   @Override
   public Iterable<String> names() {
      return hotrod.cacheManager.getCacheNames();
   }

   @Override
   public void createTemplate(String name, CacheConfiguration cacheConfiguration) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void removeTemplate(String name) {
      throw new UnsupportedOperationException();
   }

   @Override
   public Iterable<String> templateNames() {
      throw new UnsupportedOperationException();
   }

   @SuppressWarnings("unchecked")
   private <K, V> InternalRemoteCache<K, V> getCache(String name) {
      return (InternalRemoteCache<K, V>) hotrod.cacheManager.getCache(name);
   }
}
