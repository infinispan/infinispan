package org.infinispan.hotrod;

import java.util.Set;
import java.util.concurrent.CompletionStage;

import org.infinispan.api.async.AsyncCache;
import org.infinispan.api.async.AsyncCaches;
import org.infinispan.api.configuration.CacheConfiguration;
import org.infinispan.hotrod.configuration.RemoteCacheConfiguration;

/**
 * @since 14.0
 **/
public class HotRodAsyncCaches implements AsyncCaches {
   private final HotRod hotrod;

   HotRodAsyncCaches(HotRod hotrod) {
      this.hotrod = hotrod;
   }

   @Override
   public <K, V> CompletionStage<AsyncCache<K, V>> create(String name, CacheConfiguration cacheConfiguration) {
      RemoteCacheConfiguration configuration = RemoteCacheConfiguration.fromCacheConfiguration(name, cacheConfiguration);
      return hotrod.transport.<K, V>getRemoteCache(name, configuration).thenApply(r -> new HotRodAsyncCache<>(hotrod, r));
   }

   @Override
   public <K, V> CompletionStage<AsyncCache<K, V>> create(String name, String template) {
      RemoteCacheConfiguration configuration = RemoteCacheConfiguration.fromTemplate(name, template);
      return hotrod.transport.<K, V>getRemoteCache(name, configuration).thenApply(r -> new HotRodAsyncCache<>(hotrod, r));
   }

   @Override
   public <K, V> CompletionStage<AsyncCache<K, V>> get(String name) {
      return hotrod.transport.<K, V>getRemoteCache(name).thenApply(r -> new HotRodAsyncCache<>(hotrod, r));
   }

   @Override
   public CompletionStage<Void> remove(String name) {
      return hotrod.transport.removeCache(name);
   }

   @Override
   public CompletionStage<Set<String>> names() {
      return hotrod.transport.getCacheNames();
   }

   @Override
   public CompletionStage<Void> createTemplate(String name, CacheConfiguration cacheConfiguration) {
      return null;
   }

   @Override
   public CompletionStage<Void> removeTemplate(String name) {
      return null;
   }

   @Override
   public CompletionStage<Set<String>> templateNames() {
      return hotrod.transport.getTemplateNames();
   }
}
