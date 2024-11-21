package org.infinispan.client.hotrod;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

import org.infinispan.api.Experimental;
import org.infinispan.api.async.AsyncContainer;
import org.infinispan.api.async.AsyncMultimap;
import org.infinispan.api.configuration.MultimapConfiguration;

/**
 * @since 14.0
 **/
@Experimental
final class HotRodAsyncMultimap<K, V> implements AsyncMultimap<K, V> {
   private final HotRod hotrod;
   private final String name;

   HotRodAsyncMultimap(HotRod hotrod, String name) {
      this.hotrod = hotrod;
      this.name = name;
   }

   @Override
   public String name() {
      return name;
   }

   @Override
   public CompletionStage<MultimapConfiguration> configuration() {
      throw new UnsupportedOperationException();
   }

   @Override
   public AsyncContainer container() {
      return hotrod.async();
   }

   @Override
   public CompletionStage<Void> add(K key, V value) {
      throw new UnsupportedOperationException();
   }

   @Override
   public Flow.Publisher<V> get(K key) {
      throw new UnsupportedOperationException();
   }

   @Override
   public CompletionStage<Boolean> remove(K key) {
      throw new UnsupportedOperationException();
   }

   @Override
   public CompletionStage<Boolean> remove(K key, V value) {
      throw new UnsupportedOperationException();
   }

   @Override
   public CompletionStage<Boolean> containsKey(K key) {
      throw new UnsupportedOperationException();
   }

   @Override
   public CompletionStage<Boolean> containsEntry(K key, V value) {
      throw new UnsupportedOperationException();
   }

   @Override
   public CompletionStage<Long> estimateSize() {
      throw new UnsupportedOperationException();
   }
}
