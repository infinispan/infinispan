package org.infinispan.hotrod;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

import org.infinispan.api.async.AsyncMultimap;
import org.infinispan.api.configuration.MultimapConfiguration;

/**
 * @since 14.0
 **/
public class HotRodAsyncMultimap<K, V> implements AsyncMultimap<K, V> {
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
