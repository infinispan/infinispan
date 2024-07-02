package org.infinispan.embedded;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

import org.infinispan.api.async.AsyncContainer;
import org.infinispan.api.async.AsyncMultimap;
import org.infinispan.api.configuration.MultimapConfiguration;
import org.infinispan.multimap.api.embedded.MultimapCache;

/**
 * @since 15.0
 */
public class EmbeddedAsyncMultiMap<K, V> implements AsyncMultimap<K, V> {
   private final MultimapCache<K, V> multimap;
   private final Embedded embedded;

   EmbeddedAsyncMultiMap(Embedded embedded, MultimapCache<K, V> multimap) {
      this.embedded = embedded;
      this.multimap = multimap;
   }

   @Override
   public String name() {
      return multimap.getName();
   }

   @Override
   public CompletionStage<MultimapConfiguration> configuration() {
      return null;
   }

   @Override
   public AsyncContainer container() {
      return embedded.async();
   }

   @Override
   public CompletionStage<Void> add(K key, V value) {
      return multimap.put(key, value);
   }

   @Override
   public Flow.Publisher<V> get(K key) {
      return null;
   }

   @Override
   public CompletionStage<Boolean> remove(K key) {
      return multimap.remove(key);
   }

   @Override
   public CompletionStage<Boolean> remove(K key, V value) {
      return multimap.remove(key, value);
   }

   @Override
   public CompletionStage<Boolean> containsKey(K key) {
      return multimap.containsKey(key);
   }

   @Override
   public CompletionStage<Boolean> containsEntry(K key, V value) {
      return multimap.containsEntry(key, value);
   }

   @Override
   public CompletionStage<Long> estimateSize() {
      return multimap.size();
   }
}
