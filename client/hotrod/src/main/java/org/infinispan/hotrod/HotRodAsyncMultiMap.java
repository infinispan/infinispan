package org.infinispan.hotrod;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

import org.infinispan.api.async.AsyncMultiMap;
import org.infinispan.api.configuration.MultiMapConfiguration;

/**
 * @since 14.0
 **/
public class HotRodAsyncMultiMap<K, V> implements AsyncMultiMap<K, V> {
   private final HotRod hotrod;
   private final String name;

   HotRodAsyncMultiMap(HotRod hotrod, String name) {
      this.hotrod = hotrod;
      this.name = name;
   }

   @Override
   public String name() {
      return name;
   }

   @Override
   public CompletionStage<MultiMapConfiguration> configuration() {
      return null;
   }

   @Override
   public HotRodAsyncContainer container() {
      return hotrod.async();
   }

   @Override
   public CompletionStage<Void> add(K key, V value) {
      return null;
   }

   @Override
   public Flow.Publisher<V> get(K key) {
      return null;
   }

   @Override
   public CompletionStage<Boolean> remove(K key) {
      return null;
   }

   @Override
   public CompletionStage<Boolean> remove(K key, V value) {
      return null;
   }

   @Override
   public CompletionStage<Boolean> containsKey(K key) {
      return null;
   }

   @Override
   public CompletionStage<Boolean> containsEntry(K key, V value) {
      return null;
   }

   @Override
   public CompletionStage<Long> estimateSize() {
      return null;
   }
}
