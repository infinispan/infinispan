package org.infinispan.hotrod;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

import org.infinispan.api.async.AsyncCache;
import org.infinispan.api.async.AsyncCaches;
import org.infinispan.api.configuration.CacheConfiguration;
import org.infinispan.hotrod.impl.cache.RemoteCache;

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
      return hotrod.transport.getRemoteCache(name).thenApply(r -> new HotRodAsyncCache<>(hotrod, (RemoteCache<K, V>) r));
   }

   @Override
   public <K, V> CompletionStage<AsyncCache<K, V>> create(String name, String template) {
      return hotrod.transport.getRemoteCache(name).thenApply(r -> new HotRodAsyncCache<>(hotrod, (RemoteCache<K, V>) r));
   }

   @Override
   public <K, V> CompletionStage<AsyncCache<K, V>> get(String name) {
      return hotrod.transport.getRemoteCache(name).thenApply(r -> new HotRodAsyncCache<>(hotrod, (RemoteCache<K, V>) r));
   }

   @Override
   public CompletionStage<Void> remove(String name) {
      return null;
   }

   @Override
   public Flow.Publisher<String> names() {
      return null;
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
   public Flow.Publisher<String> templateNames() {
      return null;
   }
}
