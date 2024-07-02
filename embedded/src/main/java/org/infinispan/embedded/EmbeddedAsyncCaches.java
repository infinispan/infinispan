package org.infinispan.embedded;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.Cache;
import org.infinispan.api.async.AsyncCache;
import org.infinispan.api.async.AsyncCaches;
import org.infinispan.api.configuration.CacheConfiguration;
import org.infinispan.configuration.cache.Configuration;

/**
 * @since 15.0
 */
public class EmbeddedAsyncCaches implements AsyncCaches {
   private final Embedded embedded;

   EmbeddedAsyncCaches(Embedded embedded) {
      this.embedded = embedded;
   }

   @Override
   public <K, V> CompletionStage<AsyncCache<K, V>> create(String name, CacheConfiguration cacheConfiguration) {
      CompletionStage<Cache<K, V>> cache = embedded.cacheManager.administration().createCacheAsync(name, (Configuration) cacheConfiguration);
      return cache.thenApply(c -> new EmbeddedAsyncCache<>(embedded, c.getAdvancedCache()));
   }

   @Override
   public <K, V> CompletionStage<AsyncCache<K, V>> create(String name, String template) {
      CompletionStage<Cache<K, V>> cache = embedded.cacheManager.administration().createCacheAsync(name, template);
      return cache.thenApply(c -> new EmbeddedAsyncCache<>(embedded, c.getAdvancedCache()));
   }

   @Override
   public <K, V> CompletionStage<AsyncCache<K, V>> get(String name) {
      CompletionStage<Cache<K, V>> cache = embedded.cacheManager.getCacheAsync(name);
      return cache.thenApply(c -> new EmbeddedAsyncCache<>(embedded, c.getAdvancedCache()));
   }

   @Override
   public CompletionStage<Void> remove(String name) {
      return embedded.cacheManager.administration().removeCacheAsync(name);
   }

   @Override
   public CompletionStage<Set<String>> names() {
      return CompletableFuture.completedFuture(embedded.cacheManager.getCacheNames());
   }

   @Override
   public CompletionStage<Void> createTemplate(String name, CacheConfiguration cacheConfiguration) {
      return embedded.cacheManager.administration().createTemplateAsync(name, (Configuration) cacheConfiguration);
   }

   @Override
   public CompletionStage<Void> removeTemplate(String name) {
      return embedded.cacheManager.administration().removeTemplateAsync(name);
   }

   @Override
   public CompletionStage<Set<String>> templateNames() {
      return CompletableFuture.completedFuture(Collections.emptySet()); // TODO
   }
}
