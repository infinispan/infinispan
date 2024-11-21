package org.infinispan.client.hotrod;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

import org.infinispan.api.Experimental;
import org.infinispan.api.async.AsyncMultimap;
import org.infinispan.api.async.AsyncMultimaps;
import org.infinispan.api.configuration.MultimapConfiguration;

/**
 * @since 14.0
 **/
@Experimental
final class HotRodAsyncMultimaps implements AsyncMultimaps {
   private final HotRod hotrod;

   HotRodAsyncMultimaps(HotRod hotrod) {
      this.hotrod = hotrod;
   }

   @Override
   public <K, V> CompletionStage<AsyncMultimap<K, V>> create(String name, MultimapConfiguration cacheConfiguration) {
      throw new UnsupportedOperationException();
   }

   @Override
   public <K, V> CompletionStage<AsyncMultimap<K, V>> create(String name, String template) {
      throw new UnsupportedOperationException();
   }

   @Override
   public <K, V> CompletionStage<AsyncMultimap<K, V>> get(String name) {
      return CompletableFuture.completedFuture(new HotRodAsyncMultimap<>(hotrod, name)); // PLACEHOLDER
   }

   @Override
   public CompletionStage<Void> remove(String name) {
      throw new UnsupportedOperationException();
   }

   @Override
   public Flow.Publisher<String> names() {
      throw new UnsupportedOperationException();
   }

   @Override
   public CompletionStage<Void> createTemplate(String name, MultimapConfiguration cacheConfiguration) {
      throw new UnsupportedOperationException();
   }

   @Override
   public CompletionStage<Void> removeTemplate(String name) {
      throw new UnsupportedOperationException();
   }

   @Override
   public Flow.Publisher<String> templateNames() {
      throw new UnsupportedOperationException();
   }
}
