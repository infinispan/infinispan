package org.infinispan.client.hotrod;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.api.Experimental;
import org.infinispan.api.async.AsyncCache;
import org.infinispan.api.async.AsyncCaches;
import org.infinispan.api.configuration.CacheConfiguration;
import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.commons.util.concurrent.CompletableFutures;

/**
 * @since 14.0
 **/
@Experimental
final class HotRodAsyncCaches implements AsyncCaches {
   private final HotRod hotrod;

   HotRodAsyncCaches(HotRod hotrod) {
      this.hotrod = hotrod;
   }

   @Override
   public <K, V> CompletionStage<AsyncCache<K, V>> get(String name) {
      InternalRemoteCache<K, V> cache = getCache(name);
      return CompletableFuture.completedFuture(new HotRodAsyncCache<>(hotrod, cache));
   }

   @Override
   public <K, V> CompletionStage<AsyncCache<K, V>> create(String name, CacheConfiguration cacheConfiguration) {
      hotrod.cacheManager.getConfiguration().addRemoteCache(name, builder -> builder.configuration(cacheConfiguration.toString()));
      return get(name);
   }

   @Override
   public <K, V> CompletionStage<AsyncCache<K, V>> create(String name, String template) {
      throw new UnsupportedOperationException();
   }

   @Override
   public CompletionStage<Void> remove(String name) {
      hotrod.cacheManager.administration().removeCache(name);
      return CompletableFutures.completedNull();
   }

   @Override
   public CompletionStage<Set<String>> names() {
      return CompletableFuture.completedFuture(hotrod.cacheManager.getCacheNames());
   }

   @Override
   public CompletionStage<Void> createTemplate(String name, CacheConfiguration cacheConfiguration) {
      throw new UnsupportedOperationException();
   }

   @Override
   public CompletionStage<Void> removeTemplate(String name) {
      throw new UnsupportedOperationException();
   }

   @Override
   public CompletionStage<Set<String>> templateNames() {
      throw new UnsupportedOperationException();
   }

   @SuppressWarnings("unchecked")
   private <K, V> InternalRemoteCache<K, V> getCache(String name) {
      return (InternalRemoteCache<K, V>) hotrod.cacheManager.getCache(name);
   }
}
