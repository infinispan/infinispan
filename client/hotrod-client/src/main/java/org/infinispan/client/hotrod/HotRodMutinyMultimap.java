package org.infinispan.client.hotrod;

import org.infinispan.api.Experimental;
import org.infinispan.api.configuration.MultimapConfiguration;
import org.infinispan.api.mutiny.MutinyContainer;
import org.infinispan.api.mutiny.MutinyMultimap;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;

/**
 * @since 14.0
 **/
@Experimental
final class HotRodMutinyMultimap<K, V> implements MutinyMultimap<K, V> {
   private final HotRod hotrod;
   private final String name;

   HotRodMutinyMultimap(HotRod hotrod, String name) {
      this.hotrod = hotrod;
      this.name = name;
   }

   @Override
   public String name() {
      return name;
   }

   @Override
   public Uni<MultimapConfiguration> configuration() {
      throw new UnsupportedOperationException();
   }

   @Override
   public MutinyContainer container() {
      return hotrod.mutiny();
   }

   @Override
   public Uni<Void> add(K key, V value) {
      throw new UnsupportedOperationException();
   }

   @Override
   public Multi<V> get(K key) {
      throw new UnsupportedOperationException();
   }

   @Override
   public Uni<Boolean> remove(K key) {
      throw new UnsupportedOperationException();
   }

   @Override
   public Uni<Boolean> remove(K key, V value) {
      throw new UnsupportedOperationException();
   }

   @Override
   public Uni<Boolean> containsKey(K key) {
      throw new UnsupportedOperationException();
   }

   @Override
   public Uni<Boolean> containsEntry(K key, V value) {
      throw new UnsupportedOperationException();
   }

   @Override
   public Uni<Long> estimateSize() {
      throw new UnsupportedOperationException();
   }
}
