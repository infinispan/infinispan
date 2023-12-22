package org.infinispan.embedded;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

import org.infinispan.api.async.AsyncMultimap;
import org.infinispan.api.async.AsyncMultimaps;
import org.infinispan.api.configuration.MultimapConfiguration;
import org.infinispan.multimap.api.embedded.EmbeddedMultimapCacheManagerFactory;
import org.infinispan.multimap.api.embedded.MultimapCache;
import org.infinispan.multimap.api.embedded.MultimapCacheManager;

/**
 * @since 15.0
 */
public class EmbeddedAsyncMultimaps implements AsyncMultimaps {
   private final Embedded embedded;
   private final MultimapCacheManager multimapCacheManager;

   EmbeddedAsyncMultimaps(Embedded embedded) {
      this.embedded = embedded;
      this.multimapCacheManager = EmbeddedMultimapCacheManagerFactory.from(embedded.cacheManager);
   }

   @Override
   public <K, V> CompletionStage<AsyncMultimap<K, V>> create(String name, MultimapConfiguration cacheConfiguration) {
      return null;
   }

   @Override
   public <K, V> CompletionStage<AsyncMultimap<K, V>> create(String name, String template) {
      return null;
   }

   @Override
   public <K, V> CompletionStage<AsyncMultimap<K, V>> get(String name) {
      MultimapCache<K, V> multimap = multimapCacheManager.get(name);
      return CompletableFuture.completedFuture(new EmbeddedAsyncMultiMap<>(embedded, multimap));
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
   public CompletionStage<Void> createTemplate(String name, MultimapConfiguration cacheConfiguration) {
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
