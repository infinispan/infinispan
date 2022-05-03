package org.infinispan.hotrod;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

import org.infinispan.api.async.AsyncMultiMap;
import org.infinispan.api.async.AsyncMultiMaps;
import org.infinispan.api.configuration.MultiMapConfiguration;

/**
 * @since 14.0
 **/
public class HotRodAsyncMultiMaps implements AsyncMultiMaps {
   private final HotRod hotrod;

   HotRodAsyncMultiMaps(HotRod hotrod) {
      this.hotrod = hotrod;
   }

   @Override
   public <K, V> CompletionStage<AsyncMultiMap<K, V>> create(String name, MultiMapConfiguration cacheConfiguration) {
      return CompletableFuture.completedFuture(new HotRodAsyncMultiMap<>(hotrod, name)); // PLACEHOLDER
   }

   @Override
   public <K, V> CompletionStage<AsyncMultiMap<K, V>> create(String name, String template) {
      return CompletableFuture.completedFuture(new HotRodAsyncMultiMap<>(hotrod, name)); // PLACEHOLDER
   }

   @Override
   public <K, V> CompletionStage<AsyncMultiMap<K, V>> get(String name) {
      return CompletableFuture.completedFuture(new HotRodAsyncMultiMap<>(hotrod, name)); // PLACEHOLDER
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
   public CompletionStage<Void> createTemplate(String name, MultiMapConfiguration cacheConfiguration) {
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
